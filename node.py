from utils import *
from os.path import exists

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
            load_configuration(self)
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
        if msg['term'] > self._current_term \
            or (msg['term'] == self._current_term and self._voted_for in [None, msg['candidate_id']]):
            ## TODO: Add condition for candidate log being atleast as complete as local log

            self._state = "Follower"  # Becomes follower again if term is outdated
            print('Follower')

            self._current_term = msg['term']
            self._voted_for = msg['candidate_id']
            reply_vote = {
                'term': msg['candidate_id'],
                'voteGranted': True
            }
            persist_state(self)
            # self._election_timeout = self.get_election_timeout()  # ...and the node resets its election timeout.
            # self.config_timeout()

        else:
            reply_vote = {
                'term': self._current_term,
                'voteGranted': False
            }
        return pickle.dumps(reply_vote)

    def execute_state_machine(self):
        pass

    def commit(self, latestIndex):
        print('Entered commit function.')
        print(self._commit_index, latestIndex)
        for index in range(self._commit_index + 1, latestIndex + 1):
            print(index)
            commit_msg = json.dumps(self.logs[index])
            print(type(commit_msg))
            print('Commiting msg: ', commit_msg)
            with open(f'{self._name}.log', 'a') as log_file:
                log_file.write(commit_msg + '\n')
                # log_file.write('\n')
            self.execute_state_machine()
        self._commit_index = latestIndex
        print('Commited entries: ', self.logs)

    def send_commit(self, index):
        for node, value in nodos.items():
            if value['name'] != self._name:
                try:
                    print(f'Leader trying to commit {node}: {value["name"]}')
                    msg = create_commit_msg(self, index)

                    send_message(msg, value['port'])

                except Exception as e:
                    print(e)

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
                    print(f'Leader trying to append entry for {node}: {value["name"]}')
                    
                    msg = create_append_msg(self, 'apn_en', value['name'])

                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

                        # Connects to server destination
                        tcp.connect(('', value['port']))

                        print(f'Send app entry to node {node}: {value["name"]}')

                        # Send message
                        tcp.sendall(msg)

                        # Receives data from the server
                        reply = tcp.recv(1024)
                        print('Reply from node: ',reply)
                        if reply:
                            reply = pickle.loads(reply)

                            print('Append reply: ', reply)

                            if reply['success']:
                                # ack_change = reply['change']
                                # print('Recieved change: ', ack_change)
                                # print(self.next_index)
                                # print(value)
                                print('Log:', self.logs)

                                # if ack_change == self._log:
                                for i in range(len(reply['change'])):
                                    index = self.next_index[value['name']] + i
                                    # self.acks_log += 1
                                    self.ack_logs[index] += 1
                                    #if heard back from majority
                                    if self.ack_logs[index] == len(nodos)//2:
                                        self.commit(self.next_index[value['name']])
                                        # self.send_commit(self.next_index[value['name']])
                                self.next_index[value['name']] += len(reply['change'])
                            else:
                                if reply['term'] > self._current_term:
                                    self._current_term = reply['term']
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

        print(f'Node {self._name} becomes candidate')
        print('Current term:',  self._current_term)
        self._voted_for = self._name
        self._votes_in_term = 1
        persist_state(self)

        # for all nodes in cluster
        for node, value in nodos.items():
            time.sleep(0.1)
            if value['name'] != self._name:
                print(f'Trying to connect {node}: {value["name"]}')
                reply = request_vote(self, node, value)
                print('Request vote reply:', reply)
                # if not reply:
                #     break

                if reply.get('voteGranted', None):
                    self._votes_in_term += 1
                    print('Votes in term', self._votes_in_term)

                    # Once a candidate has a majority of votes it becomes leader.
                    if self._votes_in_term > len(nodos)//2:
                        self._state = 'Leader'
                        print(f'Node {self._name} becomes {self._state}')
                        self.config_leader()
                        self.append_entries()
                        return
                elif reply.get('term', None) and reply['term'] > self._current_term:
                    self._current_term = reply['term']
                    self._state = 'Follower'
                    self.config_timeout()
                    return

    def conn_loop(self, conn, address):
        with conn:
            print('Connected by', address)

            # Receives customer data
            msg = conn.recv(1024)

            # If something goes wrong with the data, it gets out of the loop
            if not msg:
                print('Nothing recieved')
                conn.close()
                return

            print('Message received: ', msg)
            # msg = msg.decode('utf-8')
            msg = pickle.loads(msg)

            # Prints the received data
            print('Msg recieved: ', msg)

            # Send the received data to the customer
            # conn.sendall(date)

            # If it is a message sent from a client
            if msg['type'] == 'client':
                client_receive_message(self, msg, conn)

            # If it is an append entry message from the leader
            elif msg['type'] == 'apn_en':
                reply_append_entry(self, msg, conn)                

            # elif msg['type'] == 'commit':
            #     self.commit()

            elif msg['type'] == 'req_vote':               
                reply_msg = self.reply_vote(msg)
                print(f'Replying to {msg["candidate_id"]}')
                conn.sendall(reply_msg)

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

    name = sys.argv[1]
    port = sys.argv[2]

    server_node = ServerNode(name, port)
