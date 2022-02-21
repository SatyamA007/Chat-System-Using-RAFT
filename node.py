import rsa
import pickle

from utils import *
from settings import *

class ServerNode:

    def __init__(self, node_id, node_port):

        self._name = node_id        # Identification of the server node
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
        self._log = ''           # log entries; each entry contains command for state machine, and term when entry was received by leader

        # Volatile state on all servers
        self._commit_index = 0   # Index of highest log entry known to be committed
        self._last_applied = 0   # Index of highest log entry applied to state machine

        # Volatile state on leaders, for each server
        self._next_index  = 0    # Index of the next log entry to send to that server, also known as last log index
        self._match_index = 0    # Index of highest log entry known to be replicated on server, also known as last log term

        self._to_commit = False

        self._ack_log = 0
        self._leader = None
        self.start()

    def start(self):

        print(f'Starting node {self._name} listening on port {self.PORT} as {self._state}')

        self.config_timeout()
        self.receive_msg()

    def config_timeout(self):

        if self._state in ['Follower', 'Candidate']:
            print(f'Configured follower node {self._name} with election timeout to: {self._election_timeout}')
            signal.signal(signal.SIGALRM, self.election_timeout_handler)
            signal.alarm(self._election_timeout)

        elif self._state == 'Leader':
            print(f'Configured candidate node {self._name} with heartbeat timeout to: {self._heartbeat_timeout}')
            signal.signal(signal.SIGALRM, self.heartbeat_timeout_handler)
            signal.alarm(self._heartbeat_timeout)

    def election_timeout_handler(self, signum, frame):
        print("Reached election timeout!")
        raise my_exceptions.ElectionException('Election')

    def heartbeat_timeout_handler(self, signum, frame):
        print("Reached hearbeat timeout!")
        raise my_exceptions.HeartbeatException('Hearbeat')

    def reply_vote(self, msg):
        """
        If the receiving node hasn't voted yet in this term then it votes for the candidate...
        :param msg:
        :return:
        """
        if msg['term'] > self._current_term:

            self._state = "Follower"  # Becomes follower again if term is outdated
            print('Follower')

            self._current_term = msg['term']
            self._voted_for = msg['candidate_id']
            reply_vote = {
                'candidate_id': msg['candidate_id']
            }
            self._election_timeout = self.get_election_timeout()  # ...and the node resets its election timeout.
            self.config_timeout()
            return pickle.dumps(reply_vote)

        else:
            reply_vote = {
                'candidate_id': self._voted_for
            }
            return pickle.dumps(reply_vote)

    def vote_in_candidate(self, candidate):
        pass

    def commit(self):
        commit_msg = str(self._log)
        print(type(commit_msg))
        print('Commiting msg: ', commit_msg)
        with open(f'{self._name}.log', 'a') as log_file:
            log_file.write(commit_msg + '\n')

    def send_commit(self):
        commit_msg = str(self._log)
        for node, value in nodos.items():
            if value['name'] != self._name:
                try:
                    print(f'Leader trying to commit {node}: {value["name"]}')
                    msg = {
                        'type': 'commit',
                        'term': self._current_term,
                        'leader_id': self._name,
                        'leader_port': self.PORT,  # AppendEntries requests include the network address of the leader
                        'prev_log_index': self._last_applied,
                        'prev_log_term': self._commit_index,
                        'leader_commit': self._to_commit,
                        'change': commit_msg
                    }
                    msg = pickle.dumps(msg)

                    send_message(msg, value['port'])

                except Exception as e:
                    print(e)

    def notify_followers(self):
        pass

    # Remote procedure call
    def append_entries(self):
        """
        Invoked by leader to replicate log entries (§5.3);
        Also used as heartbeat

        :return:
        """
        # Refreshes heartbeat
        self._heartbeat_timeout = self.get_hearbeat_timeout()
        self.config_timeout()

        for node, value in nodos.items():
            if value['name'] != self._name:
                try:
                    print(f'Leader trying to append entry for {node}: {value["name"]}')
                    msg = {
                        'type': 'apn_en',
                        'term': self._current_term,
                        'leader_id': self._name,
                        'leader_port': self.PORT,  # AppendEntries requests include the network address of the leader
                        'prev_log_index': self._last_applied,
                        'prev_log_term': self._commit_index,
                        'leader_commit': self._to_commit,
                        'change': self._log
                    }
                    msg = pickle.dumps(msg)

                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

                        # Connects to server destination
                        tcp.connect(('', value['port']))

                        print(f'Send app entry to node {node}: {value["name"]}')

                        # Send message
                        tcp.sendall(msg)

                        # Receives data from the server
                        reply = tcp.recv(4098)
                        reply = pickle.loads(reply)

                        print('Append reply: ', reply)

                        if len(reply['change']) > 0:
                            ack_change = reply['change']
                            print('Recieved change: ', ack_change)
                            print('Log:', self._log)

                            if ack_change == self._log:
                                self._ack_log += 1
                                #if heard back from majority
                                if self._ack_log > len(nodos)/2:
                                    self.commit()
                                    self.send_commit()
                                    self._ack_log = 0
                                    self._log = ''

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

        print(f'Node {self._name} becomes candidate')
        print('Current term:',  self._current_term)
        self._voted_for = self._name
        self._votes_in_term = 1

        # for all nodes in cluster
        for node, value in nodos.items():
            time.sleep(0.1)
            if value['name'] != self._name:
                print(f'Trying to connect {node}: {value["name"]}')
                reply = request_vote(self, node, value)
                
                if not reply:
                    break

                for key, value in reply.items():

                    if value == self._name:
                        self._votes_in_term += 1
                        print('Votes in term', self._votes_in_term)

                    # Once a candidate has a majority of votes it becomes leader.
                    if self._votes_in_term > len(nodos)/2:
                        self._state = 'Leader'
                        print(f'Node {self._name} becomes {self._state}')
                        self.append_entries()

    def conn_loop(self, conn, address):
        with conn:
            print('Connected by', address)

            # Receives customer data
            msg = conn.recv(4098)

            # If something goes wrong with the data, it gets out of the loop
            if not msg:
                print('Nothing recieved')
                conn.close()
                return

            msg = pickle.loads(msg)

            # Prints the received data
            print('Msg recieved: ', msg)

            # Send the received data to the customer
            # conn.sendall(date)

            # If it is a message sent from a client
            if msg['type'] == 'client':
                interface_receive_message(self, msg, conn)

            # If it is an append entry message from the leader
            elif msg['type'] == 'apn_en':
                reply_append_entry(self, msg, conn)                

            elif msg['type'] == 'commit':
                self.commit()
                self._log = ''

            elif msg['type'] == 'req_vote':               
                reply_msg = self.reply_vote(msg)
                print(f'Replying to {msg["candidate_id"]}')
                conn.sendall(reply_msg)

            #Below a node processes all known commands sent to it via interface
            elif msg['type'] == 'create_group':
                publicKey_group,privateKey_group = rsa.newkeys(16) 
                print()
                publicKey_candidates = [fetch_key_pairs(x)[0] for x in msg['client_ids'] ]
                log_entry = {
                    'type':'client',
                    'change': { 
                        'term': self._current_term,
                        'group_id': msg['group_id'],
                        'client_ids': msg['client_ids'],
                        'group_public_key': publicKey_group,
                        'private_key_encrypted': [encrypt_group_key(x, privateKey_group) for x in publicKey_candidates]
                    }
                }
                decrypt_group_key(log_entry['change']['private_key_encrypted'][0],log_entry['change']['client_ids'][0] )
                interface_receive_message(self, log_entry, conn)

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

                    print('created socket')
                    self.conn_loop(self._conn, self._address)

                except my_exceptions.ElectionException:
                    print('Starting new election')
                    self.start_election()

                except my_exceptions.HeartbeatException:
                    print('Appending entries')
                    self.append_entries()

    def get_election_timeout(self):
        """
        Set a new election timeout for follower node

        :return: timeout between 3 and 5 seconds
        """
        election_timeout = round(random.uniform(5, 8))
        print(f'Node {self._name} have new election timeout of {election_timeout}')
        return election_timeout

    def get_hearbeat_timeout(self):
        """
         Set a hearbeat  timeout for leader node
        :return: timeout of two seconds
        """
        return heartbeatInterval


if __name__ == "__main__":
    name = 'not_defined'
    if len(sys.argv)>1:
        name = str(sys.argv[1])

    while name not in nodos.keys():
        name = input("Provide server name (1-5): ")

    server_node = ServerNode(chr(ord(name)-ord('1')+ord('a')), nodos[name]['port'])
