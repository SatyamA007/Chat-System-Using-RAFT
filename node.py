import rsa
from rsa import PublicKey
import pickle

from os.path import exists
from utils import *
from settings import *

class ServerNode:

    def __init__(self, node_id, name, node_port):

        self._name = name        # Identification of the server node
        self._id = node_id        # Identification of the server node
        self._state = 'Follower'    # Every server starts as follower

        self.PORT = int(node_port)   # Arbitrary port for the server
        self._conn = None
        self._address = None

        # The election timeout is the amount of time a follower waits until becoming a candidate.
        self._election_timeout = self.get_election_timeout()  # Sets node election timeout
        self._votes_in_term = 0      # Votes received by the candidate in a given election term
        self._heartbeat_timeout = self.get_hearbeat_timeout()  # Must be less then election timeout

        # Persistent state on all servers
        self._current_term = 0   # Latest term server has seen
        self._voted_for = None   # Candidate id that received vote in current term
        self.logs = []
        self.ack_logs = []
        self.next_index = None

        # Volatile state on all servers
        self._commit_index = -1   # Index of highest log entry known to be committed
        self._last_applied = 0   # Index of highest log entry applied to state machine

        # # Volatile state on leaders, for each server
        # self._next_index  = 0    # Index of the next log entry to send to that server, also known as last log index
        # self._match_index = 0    # Index of highest log entry known to be replicated on server, also known as last log term

        self._leader = None
        self.recover_state()
        self.start()

    def recover_state(self):
        if exists(self._name + '.state'):
            load_state(self)
            print('State recovered successfully.')

        if exists(self._name + '.log'):
            load_logs(self)
        else:
            # create log file if it doesn't exist
            with open(self._name + '.log', 'w') as fp:
                pass
        

    def start(self):

        print(f'Starting node {self._name} listening on port {self.PORT} as {self._state}')

        self.config_timeout()
        self.receive_msg()

    def config_timeout(self):

        if self._state in ['Follower', 'Candidate']:
            print(f'Configured follower node {self._name} with election timeout to: {self._election_timeout}')  if DEBUGGING_ON else None
            signal.signal(signal.SIGALRM, self.election_timeout_handler)
            signal.alarm(self._election_timeout)

        elif self._state == 'Leader':
            print(f'Configured candidate node {self._name} with heartbeat timeout to: {self._heartbeat_timeout}')  if DEBUGGING_ON else None
            signal.signal(signal.SIGALRM, self.heartbeat_timeout_handler)
            signal.alarm(self._heartbeat_timeout)

    def election_timeout_handler(self, signum, frame):
        print("Reached election timeout!")  if DEBUGGING_ON else None
        raise my_exceptions.ElectionException('Election')

    def heartbeat_timeout_handler(self, signum, frame):
        print("Reached hearbeat timeout!")  if DEBUGGING_ON else None
        raise my_exceptions.HeartbeatException('Hearbeat')

    def reply_vote(self, msg):
        """
        If the receiving node hasn't voted yet in this term then it votes for the candidate...
        :param msg:
        :return:
        """
        if msg['term'] > self._current_term \
            or (msg['term'] == self._current_term and self._voted_for in [None, msg['candidate_id']] \
            and msg['last_log_index'] >= len(self.logs) - 1):

            self._state = "Follower"  # Becomes follower again if term is outdated
            print('Follower')  if DEBUGGING_ON else None

            self._current_term = msg['term']
            self._voted_for = msg['candidate_id']
            reply_vote = {
                'term': msg['candidate_id'],
                'voteGranted': True
            }
            persist_state(self)

        else:
            reply_vote = {
                'term': self._current_term,
                'voteGranted': False
            }
        return pickle.dumps(reply_vote)

    def execute_state_machine(self):
        pass

    def commit(self, latestIndex):
        print(self._commit_index, latestIndex)
        for index in range(self._commit_index + 1, latestIndex + 1):
            self.execute_state_machine()
            # commit_msg = pickle.dumps(self.logs[index])
            # print(type(commit_msg)) if DEBUGGING_ON else None
            # print('Commiting msg: ', commit_msg)
            # with open(f'{self._name}.log', 'a') as log_file:
            #     log_file.write(commit_msg)
            #     log_file.write('\n')
        with open(f'{self._name}.log', 'wb') as log_file:
            pickle.dump(self.logs[:latestIndex + 1], log_file)
            
        self._commit_index = latestIndex

    def notify_followers(self):
        pass

    def config_leader(self):
        # Refreshes heartbeat
        self._heartbeat_timeout = self.get_hearbeat_timeout()
        self.next_index = {}
        for val in nodos.values():
            if val['name'] != self._name:
                self.next_index[val['name']] = len(self.logs)

    # Remote procedure call
    def append_entries(self):
        """
        Invoked by leader to replicate log entries (§5.3);
        Also used as heartbeat

        :return:
        """
        self.config_timeout()
        
        for node, value in nodos.items():
            if value['name'] != self._name:
                try:
                    print(f'Leader trying to append entry for {node}: {value["name"]}') if DEBUGGING_ON else None
                    
                    msg = create_append_msg(self, 'apn_en', value['name'])
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

                        # Connects to server destination
                        tcp.connect(('', value['port']))

                        print(f'Send app entry to node {node}: {value["name"]}') if DEBUGGING_ON else None

                        # Send message
                        tcp.sendall(msg)

                        # Receives data from the server
                        reply = tcp.recv(4098)
                        if reply:
                            reply = pickle.loads(reply)

                            print('Append reply: ', reply)

                            if reply['success']:
                                print('Log:', self.logs) if DEBUGGING_ON else None

                                for i in range(len(reply['change'])):
                                    index = self.next_index[value['name']] + i
                                    self.ack_logs[index] += 1
                                    #if heard back from majority
                                    if self.ack_logs[index] == len(nodos)//2:
                                        self.commit(self.next_index[value['name']])
                                self.next_index[value['name']] += len(reply['change'])

                            else:
                                if reply['term'] > self._current_term:
                                    self._current_term = reply['term']
                                    self._voted_for = None
                                    persist_state(self)
                                    self._state = 'Follower'
                                    self.config_timeout()
                                    return
                                self.next_index[value['name']] -= 1
                except Exception as e:
                    print(e)
        
    def start_election(self):
        """
        Starts a new election term
        :return:
        """
        self._state = 'Candidate'
        self._current_term += 1
        self.config_timeout()

        print(f'Node {self._name} becomes candidate')  if DEBUGGING_ON else None
        print('Current term:',  self._current_term)  if DEBUGGING_ON else None
        self._voted_for = self._name
        self._votes_in_term = 1
        persist_state(self)

        # for all nodes in cluster
        for node, value in nodos.items():
            time.sleep(0.1)
            if value['name'] != self._name:
                print(f'Trying to connect {node}: {value["name"]}')  if DEBUGGING_ON else None
                reply = request_vote(self, node, value)
                print('Request vote reply:', reply)
                # if not reply:
                #     break

                if reply.get('voteGranted', None):
                    self._votes_in_term += 1
                    print('Votes in term', self._votes_in_term) if DEBUGGING_ON else None

                    # Once a candidate has a majority of votes it becomes leader.
                    if self._votes_in_term > len(nodos)//2:
                        self._state = 'Leader'
                        print(f'Node {self._name} becomes {self._state}')
                        self.config_leader()
                        self.append_entries()
                        return
                elif reply.get('term', None) and reply['term'] > self._current_term:
                    self._current_term = reply['term']
                    self._voted_for = None
                    persist_state(self)
                    self._state = 'Follower'
                    self.config_timeout()
                    return

    def conn_loop(self, conn, address):
        with conn:
            print('Connected by', address) if DEBUGGING_ON else None

            # Receives customer data
            msg = conn.recv(4098)

            # If something goes wrong with the data, it gets out of the loop
            if not msg:
                print('Nothing recieved')  if DEBUGGING_ON else None
                conn.close()
                return

            msg = pickle.loads(msg)
            # Prints the received data
            print('Msg received: ', msg)  if DEBUGGING_ON else None

            # If it is a message sent from a client
            if msg['type'] == 'client':
                interface_receive_message(self, msg)

            # If it is an append entry message from the leader
            elif msg['type'] == 'apn_en':
                reply_append_entry(self, msg, conn)                

            elif msg['type'] == 'req_vote':               
                reply_msg = self.reply_vote(msg)
                print(f'Replying to {msg["candidate_id"]}')  if DEBUGGING_ON else None
                conn.sendall(reply_msg)

            #Below a node processes all known commands sent to it via interface
            elif msg['type'] == 'create_group':
                signal.alarm(0)
                publicKey_group,privateKey_group = rsa.newkeys(GROUP_KEY_NBITS) 

                log_entry = {
                    'type':'client',
                    'change': { 
                        'term': self._current_term,
                        'type': 'createGroup',
                        'group_id': msg['group_id'],
                        'client_ids': msg['client_ids'],
                        'group_public_key': publicKey_group,
                        'private_key_encrypted': [encrypt_group_key(x, privateKey_group) for x in msg['client_ids']]
                    }
                }
                interface_receive_message(self, log_entry)
                self.config_timeout()
            elif msg['type'] == 'add2group':
                #TODO: Add method to find the group, return latest entry if it exists
                log_for_g_id = {'term': 13, 'type': 'createGroup', 'group_id': 'g1', 'client_ids': ['2', '1'], 'group_public_key': PublicKey(86702799256623328332783554034575463517726992144676035689201744930242486124477, 65537), 'private_key_encrypted': [b'(\xfd\xb5\xd0\xde\xe1r\xdd\x10N\xf3\t\xb7\xea1\xab\xc2Hw!U\xab\xe6qd\x9d6\x88\xdd*\x1dFv"\x13\xfbg\xa5,\x87^\xc8\xcb\xf18\xe8/\x90\xc7\xad\xb8Q\x1f\xc3\xef\x17\x87Vj\x08\xfb\x82\xf4\x8aL\x99\xbaX\x81\xcb\x1b\x87_\x9b\xa4\xcas\x15\xa9\x0c\x17\x01\x00D\\LO\xbf\xb0\x1a\x93gv-\x89\x9fgI\x9a%\xb5\xb5\xb6\xeb\xab\xc4T:q\x00\x01\xc8\x96\xd0\xf2\xfa\x9c\x0e\x10p\xdd\xe3\xed}\x08\x98Z\xfe\xaen\x10\xeb\xf0 \xafM\x89\xf9\xdd\x1e\x8e\x93X\xf1S\xb9\x17\xd0O\xba\r\x03\xeddL\xf1)C=|\xd8\t\xd8\xdczS\x1dDJ\x94\xb9l\xb6\xd6N\xbc\x9d\xaf\x1f\x16&\xd1\x9b\x7f\'\x06x.\xbc|\xe7\xf5\xdbn\xc7\xd4x\'w\\\xf4 Wrg;,f\xa0(\xaaS\x82\xa6\xda\x02\x01\xca\n\x87]K\xa3\x0b\x08\x98\xc5\xfe\xe5\x89b\xcd\x14\x96cF\xa2\xb2\xee\x93\x83h\x9c,\xefw\xcbi\xb2rV\xbe,\xd1\x8b\x87', b'\x00\x02\x82\x99\xccqK\x9f]\xd5\x0c\x83\x8ar\xc1\x8c0up\xb9F^\x93\xeb\x90\xaeTz\xbb\xd0^\xf4\x87/\rq\x8f\xe6\xef\x85\x87\x00\x80\x04\x95\xcb\x00\x00\x00\x00\x00\x00\x00\x8c\x07rsa.key\x94\x8c\nPrivateKey\x94\x93\x94)\x81\x94(\x8a!\xbd\xbf\xf8\x12ln\xd9$\x16!\xba\x0e6\x18\xc8\xb8\xda1\x88\x0c\xed7\x9c\xb78uG\xd8\x9a\x0b\xb0\xbf\x00J\x01\x00\x01\x00\x8a!\x193+\x11\xed57T?H\xa5<\xad\xb8I\xe6EF\xbb\x87\x83r$Bq\xc2\x80/\x17W\x04\xa9\x00\x8a\x12\x9f*6\xcb\xa8\xb3\xaa\xa3\xd0\x91C\xf0\xb6\xa5\t\x9b\xde\x00\x8a\x10#\x94\xf3[4\xa5\xe9pj\x8c\x86\x03\xa2q\xdc\x00\x8a\x12A\x7f\\#[i\x8aQ\xb6!\x9f\xabDa\xa0\xb6\x89\x00\x8a\x0f\r\xb1\xaa$\xf0=,t5\x81\x93\x80E\x122\x8a\x12\x947m8p\xf4\xa8\x1a\xbe\xa8\x87HM<\xb9\x8d\xd5\x00t\x94b.']}

                if log_for_g_id and self._id in log_for_g_id['client_ids'] and msg['node'] not in log_for_g_id['client_ids']: #check if this client currently in the group 
                    
                    self_idx_in_group = log_for_g_id['client_ids'].index(self._id)
                    privateKey_group = decrypt_group_key(log_for_g_id['private_key_encrypted'][self_idx_in_group], self._id)

                    log_for_g_id['private_key_encrypted'].append(encrypt_group_key(msg['node'], privateKey_group))
                    log_for_g_id['client_ids'].append(msg['node'])
                    
                    log_entry = {
                    'type':'client',
                    'change': { 
                        'term': self._current_term,
                        'type': 'add',
                        'group_id': log_for_g_id['group_id'],
                        'client_ids': log_for_g_id['client_ids'],
                        'group_public_key': log_for_g_id['group_public_key'],
                        'private_key_encrypted': log_for_g_id['private_key_encrypted'],
                        'added_member':msg['node']
                        }
                    }
                    interface_receive_message(self, log_entry)
                else:
                    if not log_for_g_id:
                        send_message({'Error adding memeber':'Group with ID = '+msg['group_id']+' does not exist!', 'Commit Status':'Aborted'}, interface['port'])
                    elif msg['node'] in log_for_g_id['client_ids']:
                        send_message({'Error adding memeber':'Client '+ msg['node']+' already a member of '+msg['group_id'], 'Commit Status':'Aborted'}, interface['port'])
                    else:
                        send_message({'Error adding memeber':'Client in-charge not a member of '+msg['group_id'], 'Commit Status':'Aborted'}, interface['port'])
            
            elif msg['type'] == 'kick':
                #TODO: Add method to find the group, return latest entry if it exists
                log_for_g_id = {'term': 13, 'type': 'createGroup', 'group_id': 'g1', 'client_ids': ['2', '1'], 'group_public_key': PublicKey(86702799256623328332783554034575463517726992144676035689201744930242486124477, 65537), 'private_key_encrypted': [b'(\xfd\xb5\xd0\xde\xe1r\xdd\x10N\xf3\t\xb7\xea1\xab\xc2Hw!U\xab\xe6qd\x9d6\x88\xdd*\x1dFv"\x13\xfbg\xa5,\x87^\xc8\xcb\xf18\xe8/\x90\xc7\xad\xb8Q\x1f\xc3\xef\x17\x87Vj\x08\xfb\x82\xf4\x8aL\x99\xbaX\x81\xcb\x1b\x87_\x9b\xa4\xcas\x15\xa9\x0c\x17\x01\x00D\\LO\xbf\xb0\x1a\x93gv-\x89\x9fgI\x9a%\xb5\xb5\xb6\xeb\xab\xc4T:q\x00\x01\xc8\x96\xd0\xf2\xfa\x9c\x0e\x10p\xdd\xe3\xed}\x08\x98Z\xfe\xaen\x10\xeb\xf0 \xafM\x89\xf9\xdd\x1e\x8e\x93X\xf1S\xb9\x17\xd0O\xba\r\x03\xeddL\xf1)C=|\xd8\t\xd8\xdczS\x1dDJ\x94\xb9l\xb6\xd6N\xbc\x9d\xaf\x1f\x16&\xd1\x9b\x7f\'\x06x.\xbc|\xe7\xf5\xdbn\xc7\xd4x\'w\\\xf4 Wrg;,f\xa0(\xaaS\x82\xa6\xda\x02\x01\xca\n\x87]K\xa3\x0b\x08\x98\xc5\xfe\xe5\x89b\xcd\x14\x96cF\xa2\xb2\xee\x93\x83h\x9c,\xefw\xcbi\xb2rV\xbe,\xd1\x8b\x87', b'\x00\x02\x82\x99\xccqK\x9f]\xd5\x0c\x83\x8ar\xc1\x8c0up\xb9F^\x93\xeb\x90\xaeTz\xbb\xd0^\xf4\x87/\rq\x8f\xe6\xef\x85\x87\x00\x80\x04\x95\xcb\x00\x00\x00\x00\x00\x00\x00\x8c\x07rsa.key\x94\x8c\nPrivateKey\x94\x93\x94)\x81\x94(\x8a!\xbd\xbf\xf8\x12ln\xd9$\x16!\xba\x0e6\x18\xc8\xb8\xda1\x88\x0c\xed7\x9c\xb78uG\xd8\x9a\x0b\xb0\xbf\x00J\x01\x00\x01\x00\x8a!\x193+\x11\xed57T?H\xa5<\xad\xb8I\xe6EF\xbb\x87\x83r$Bq\xc2\x80/\x17W\x04\xa9\x00\x8a\x12\x9f*6\xcb\xa8\xb3\xaa\xa3\xd0\x91C\xf0\xb6\xa5\t\x9b\xde\x00\x8a\x10#\x94\xf3[4\xa5\xe9pj\x8c\x86\x03\xa2q\xdc\x00\x8a\x12A\x7f\\#[i\x8aQ\xb6!\x9f\xabDa\xa0\xb6\x89\x00\x8a\x0f\r\xb1\xaa$\xf0=,t5\x81\x93\x80E\x122\x8a\x12\x947m8p\xf4\xa8\x1a\xbe\xa8\x87HM<\xb9\x8d\xd5\x00t\x94b.']}

                if log_for_g_id and self._id in log_for_g_id['client_ids'] and msg['node'] in log_for_g_id['client_ids']: #check if this client currently in the group 
                    publicKey_group,privateKey_group = rsa.newkeys(GROUP_KEY_NBITS) 
                    
                    log_for_g_id['client_ids'].remove(msg['node'])
                    log_for_g_id['private_key_encrypted']= [encrypt_group_key(x, privateKey_group) for x in log_for_g_id['client_ids']]
                    
                    log_entry = {
                    'type':'client',
                    'change': { 
                        'term': self._current_term,
                        'type': 'kick',
                        'group_id': log_for_g_id['group_id'],
                        'client_ids': log_for_g_id['client_ids'],
                        'group_public_key': publicKey_group,
                        'private_key_encrypted': log_for_g_id['private_key_encrypted'],
                        'kicked_member':msg['node']
                        }
                    }
                    interface_receive_message(self, log_entry)
                else:
                    if not log_for_g_id:
                        send_message({'Error kicking memeber':'Group with ID = '+msg['group_id']+' does not exist!', 'Commit Status':'Aborted'}, interface['port'])
                    elif msg['node'] not in log_for_g_id['client_ids']:
                        send_message({'Error kicking memeber':'Client '+ msg['node']+' not a member of '+msg['group_id'], 'Commit Status':'Aborted'}, interface['port'])
                    else:
                        send_message({'Error kicking memeber':'Client in-charge not a member of '+msg['group_id'], 'Commit Status':'Aborted'}, interface['port'])
            
            elif msg['type']=='write_message':
                #TODO: Add method to find the group, return latest entry if it exists
                log_for_g_id = {'term': 13, 'type': 'createGroup', 'group_id': 'g1', 'client_ids': ['2', '1'], 'group_public_key': PublicKey(86702799256623328332783554034575463517726992144676035689201744930242486124477, 65537), 'private_key_encrypted': [b'(\xfd\xb5\xd0\xde\xe1r\xdd\x10N\xf3\t\xb7\xea1\xab\xc2Hw!U\xab\xe6qd\x9d6\x88\xdd*\x1dFv"\x13\xfbg\xa5,\x87^\xc8\xcb\xf18\xe8/\x90\xc7\xad\xb8Q\x1f\xc3\xef\x17\x87Vj\x08\xfb\x82\xf4\x8aL\x99\xbaX\x81\xcb\x1b\x87_\x9b\xa4\xcas\x15\xa9\x0c\x17\x01\x00D\\LO\xbf\xb0\x1a\x93gv-\x89\x9fgI\x9a%\xb5\xb5\xb6\xeb\xab\xc4T:q\x00\x01\xc8\x96\xd0\xf2\xfa\x9c\x0e\x10p\xdd\xe3\xed}\x08\x98Z\xfe\xaen\x10\xeb\xf0 \xafM\x89\xf9\xdd\x1e\x8e\x93X\xf1S\xb9\x17\xd0O\xba\r\x03\xeddL\xf1)C=|\xd8\t\xd8\xdczS\x1dDJ\x94\xb9l\xb6\xd6N\xbc\x9d\xaf\x1f\x16&\xd1\x9b\x7f\'\x06x.\xbc|\xe7\xf5\xdbn\xc7\xd4x\'w\\\xf4 Wrg;,f\xa0(\xaaS\x82\xa6\xda\x02\x01\xca\n\x87]K\xa3\x0b\x08\x98\xc5\xfe\xe5\x89b\xcd\x14\x96cF\xa2\xb2\xee\x93\x83h\x9c,\xefw\xcbi\xb2rV\xbe,\xd1\x8b\x87', b'\x00\x02\x82\x99\xccqK\x9f]\xd5\x0c\x83\x8ar\xc1\x8c0up\xb9F^\x93\xeb\x90\xaeTz\xbb\xd0^\xf4\x87/\rq\x8f\xe6\xef\x85\x87\x00\x80\x04\x95\xcb\x00\x00\x00\x00\x00\x00\x00\x8c\x07rsa.key\x94\x8c\nPrivateKey\x94\x93\x94)\x81\x94(\x8a!\xbd\xbf\xf8\x12ln\xd9$\x16!\xba\x0e6\x18\xc8\xb8\xda1\x88\x0c\xed7\x9c\xb78uG\xd8\x9a\x0b\xb0\xbf\x00J\x01\x00\x01\x00\x8a!\x193+\x11\xed57T?H\xa5<\xad\xb8I\xe6EF\xbb\x87\x83r$Bq\xc2\x80/\x17W\x04\xa9\x00\x8a\x12\x9f*6\xcb\xa8\xb3\xaa\xa3\xd0\x91C\xf0\xb6\xa5\t\x9b\xde\x00\x8a\x10#\x94\xf3[4\xa5\xe9pj\x8c\x86\x03\xa2q\xdc\x00\x8a\x12A\x7f\\#[i\x8aQ\xb6!\x9f\xabDa\xa0\xb6\x89\x00\x8a\x0f\r\xb1\xaa$\xf0=,t5\x81\x93\x80E\x122\x8a\x12\x947m8p\xf4\xa8\x1a\xbe\xa8\x87HM<\xb9\x8d\xd5\x00t\x94b.']}

                if log_for_g_id:
                    encrypted_msg = rsa.encrypt(msg['message'].encode('utf8'), log_for_g_id['group_public_key'])
                    log_entry = {
                        'type':'client',
                        'change': { 
                            'term': self._current_term,
                            'type': 'message',
                            'group_id': log_for_g_id['group_id'],
                            'client_ids': log_for_g_id['client_ids'],
                            'group_public_key': log_for_g_id['group_public_key'],
                            'private_key_encrypted':log_for_g_id['private_key_encrypted'],
                            'sender': self._id,
                            'message':encrypted_msg
                        }
                    }
                    interface_receive_message(self, log_entry)
                else:
                    send_message({'Error writing to group': 'Group with id = '+msg['group_id']+' does not exist!', 'Commit Status':'Aborted'}, interface['port'])
            
            elif msg['type'] == 'print_group':
                                
                my_logs = [{'term': 26, 'type': 'message', 'group_id': 'g1', 'client_ids': ['2', '1'], 'group_public_key': PublicKey(86702799256623328332783554034575463517726992144676035689201744930242486124477, 65537), 'sender': '1', 'message': b"\x14?\xa8\xb2\x91\x11\x1fH\xb4\xad3\x1dz9\x1ed{\x15'\xbd\x9bSg\x88\x9d&\xbb\xd4\xd5N\xffu", 'private_key_encrypted': [b'(\xfd\xb5\xd0\xde\xe1r\xdd\x10N\xf3\t\xb7\xea1\xab\xc2Hw!U\xab\xe6qd\x9d6\x88\xdd*\x1dFv"\x13\xfbg\xa5,\x87^\xc8\xcb\xf18\xe8/\x90\xc7\xad\xb8Q\x1f\xc3\xef\x17\x87Vj\x08\xfb\x82\xf4\x8aL\x99\xbaX\x81\xcb\x1b\x87_\x9b\xa4\xcas\x15\xa9\x0c\x17\x01\x00D\\LO\xbf\xb0\x1a\x93gv-\x89\x9fgI\x9a%\xb5\xb5\xb6\xeb\xab\xc4T:q\x00\x01\xc8\x96\xd0\xf2\xfa\x9c\x0e\x10p\xdd\xe3\xed}\x08\x98Z\xfe\xaen\x10\xeb\xf0 \xafM\x89\xf9\xdd\x1e\x8e\x93X\xf1S\xb9\x17\xd0O\xba\r\x03\xeddL\xf1)C=|\xd8\t\xd8\xdczS\x1dDJ\x94\xb9l\xb6\xd6N\xbc\x9d\xaf\x1f\x16&\xd1\x9b\x7f\'\x06x.\xbc|\xe7\xf5\xdbn\xc7\xd4x\'w\\\xf4 Wrg;,f\xa0(\xaaS\x82\xa6\xda\x02\x01\xca\n\x87]K\xa3\x0b\x08\x98\xc5\xfe\xe5\x89b\xcd\x14\x96cF\xa2\xb2\xee\x93\x83h\x9c,\xefw\xcbi\xb2rV\xbe,\xd1\x8b\x87', b'\x00\x02\x82\x99\xccqK\x9f]\xd5\x0c\x83\x8ar\xc1\x8c0up\xb9F^\x93\xeb\x90\xaeTz\xbb\xd0^\xf4\x87/\rq\x8f\xe6\xef\x85\x87\x00\x80\x04\x95\xcb\x00\x00\x00\x00\x00\x00\x00\x8c\x07rsa.key\x94\x8c\nPrivateKey\x94\x93\x94)\x81\x94(\x8a!\xbd\xbf\xf8\x12ln\xd9$\x16!\xba\x0e6\x18\xc8\xb8\xda1\x88\x0c\xed7\x9c\xb78uG\xd8\x9a\x0b\xb0\xbf\x00J\x01\x00\x01\x00\x8a!\x193+\x11\xed57T?H\xa5<\xad\xb8I\xe6EF\xbb\x87\x83r$Bq\xc2\x80/\x17W\x04\xa9\x00\x8a\x12\x9f*6\xcb\xa8\xb3\xaa\xa3\xd0\x91C\xf0\xb6\xa5\t\x9b\xde\x00\x8a\x10#\x94\xf3[4\xa5\xe9pj\x8c\x86\x03\xa2q\xdc\x00\x8a\x12A\x7f\\#[i\x8aQ\xb6!\x9f\xabDa\xa0\xb6\x89\x00\x8a\x0f\r\xb1\xaa$\xf0=,t5\x81\x93\x80E\x122\x8a\x12\x947m8p\xf4\xa8\x1a\xbe\xa8\x87HM<\xb9\x8d\xd5\x00t\x94b.']}]

                messages_with_gid = []

                for log in my_logs:
                    if log['group_id'] == msg['group_id'] and log['type'] == 'message' and self._id in log['client_ids']:
                        my_idx_in_group = log['client_ids'].index(self._id)
                        privateKey_group = decrypt_group_key(log['private_key_encrypted'][my_idx_in_group], self._id)
                        decrypted_message = decrypt_message(log['message'], privateKey_group)
                        messages_with_gid.append({'message': decrypted_message,'sender': log['sender']} )
                #Even if the response seems empty, the group may still have messages from external senders, 
                # or this client might be that external sender but does not have the private key for decryption
                send_message(messages_with_gid, interface['port']) if messages_with_gid else send_message("Empty response: Given client "+self._id+" cannot decypher messages for group "+msg['group_id']+" OR no such group exists.", interface['port'])

    def receive_msg(self):

        print('creating socket')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

            # Join socket to host and port
            tcp.bind(('', self.PORT)) # Receives messages from any host

            # Enables the server to accept 5 connections
            tcp.listen(5)

            while True:

                try:
                    # Accepts a connection. Returns the socket object (conn) and client address (address)
                    self._conn, self._address = tcp.accept()

                    print('created socket') if DEBUGGING_ON else None
                    self.conn_loop(self._conn, self._address)

                except my_exceptions.ElectionException:
                    print('Starting new election') if DEBUGGING_ON else None
                    self.start_election()

                except my_exceptions.HeartbeatException:
                    print('Appending entries') if DEBUGGING_ON else None
                    self.append_entries()

    def get_election_timeout(self):
        """
        Set a new election timeout for follower node

        :return: timeout between 3 and 5 seconds
        """
        election_timeout = round(random.uniform(5, 8))
        print(f'Node {self._name} have new election timeout of {election_timeout}') if DEBUGGING_ON else None
        return election_timeout

    def get_hearbeat_timeout(self):
        """
         Set a hearbeat  timeout for leader node
        :return: timeout of two seconds
        """
        return heartbeatInterval


if __name__ == "__main__":
    client_id = 'not_defined'
    if len(sys.argv)>1:
        client_id = str(sys.argv[1])

    while client_id not in nodos.keys():
        client_id = input("Provide server id (1-5): ")

    server_node = ServerNode(client_id, id_to_name(client_id), nodos[client_id]['port'])
