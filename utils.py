import random
import time
import socket
import signal
import sys
import pickle
import json
import my_exceptions

from settings import *

def create_append_msg(self, type, node):
    logs = self.logs[self.next_index[node]:]
    msg = {
        'type': type,
        'term': self._current_term,
        'leader_id': self._name,
        'leader_port': self.PORT,  # AppendEntries requests include the network address of the leader
        'prev_log_index': self.next_index[node] - 1,
        'prev_log_term': self.logs[self.next_index[node] - 1]['term'] if self.next_index[node] > 0 else None,
        'change': logs,
        'commit_index': self._commit_index
    }
    return pickle.dumps(msg)

def create_commit_msg(self, log):
    msg = {
        'type': type,
        'term': self._current_term,
        'leader_id': self._name,
        'leader_port': self.PORT,  # AppendEntries requests include the network address of the leader
        'prev_log_index': self.next_index[node] - 1,
        'prev_log_term': self.logs[self.next_index[node] - 1]['term'] if self.next_index[node] > 0 else None,
        'change': log
    }
    return pickle.dumps(msg)

def client_receive_message(self, msg, conn):
    print('Recieved msg from client')

    # Only the leader handles it
    if self._state == 'Leader':  # This process is called Log Replication
        # change goes to the leader

        print('Leader append log: ', msg['change'])
        log = {
            'term': self._current_term,
            'data': msg['change']
        }
        self.logs.append(log)  # Each change is added as an entry in the nodes's log
        self.ack_logs.append(0)

    # This log entry is currently uncommitted so it won't update the node's value.

    # To commit the entry the node first replicates it to the follower nodes...
    # Then the leader waits until a majority of nodes have written the entry.
    # The entry is now committed on the leader node and the node state is "X"
    # The leader then notifies the followers that the entry is committed.
    # The cluster has now come to consensus about the system state.

    # If a follower receives a message from a client the it must redirect to the leader
    else:
        redirect_to_leader(self, msg, conn)

def redirect_to_leader(self, msg, conn):
    next_node_port = (self.PORT - 5000)%(len(nodos)) + 5001
    conn.sendall((pickle.dumps({'Not a leader': 'redirecting to port ' + str(next_node_port) })))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
        # Connects to server destination
        tcp.connect(('', next_node_port))
        # Send message
        tcp.sendall(pickle.dumps(msg))

def reply_append_entry(self, msg, conn):
    """
    An entry is committed once a majority of followers acknowledge it...
    :param append_entry_msg:
    :return:
    """
    ack_msg = {
        'term': self._current_term,
        'success': False,
        'change': msg['change']
    }
    if msg['term'] < self._current_term:
        pass
    else:
        if msg['term'] > self._current_term:
            self._current_term = msg['term']
        self._state = "Follower"
        self._election_timeout = self.get_election_timeout()
        self.config_timeout()
        #  or \
        # (msg['term'] == self._current_term and (not msg['prev_log_term']) or (len(self.logs) > msg['prev_log_index']) \
        # and msg['prev_log_term'] == self.logs[msg['prev_log_index']]['term']):
        print('Message: ', msg)
        if (not msg['prev_log_term'] or (msg['prev_log_index'] < len(self.logs) and \
            msg['prev_log_term'] == self.logs[msg['prev_log_index']]['term'])):

            # No reply, when no change (heartbeat message)
            if not len(msg['change']):
                self.commit(msg['commit_index'])
                return
            self.logs = self.logs[:msg['prev_log_index'] + 1]
            self.ack_logs = self.ack_logs[:len(self.logs)]
            self.logs.extend(msg['change'])
            self.ack_logs.extend([0]*len(msg['change']))
            ack_msg['success'] = True
            self.commit(msg['commit_index'])

            print('Append entry successful')
            print('Log: ', self.logs)
            
    ##TODO: Append any new entries not already in the log
    ##TODO: Forward state machine
    reply = pickle.dumps(ack_msg)
    conn.sendall(reply)


# Remote procedure call
def request_vote(self, node, value):

    msg = {
        'type': 'req_vote',
        'term': self._current_term,
        'candidate_id': self._name,
        'last_log_index': len(self.logs) - 1,
        'last_log_term': self.logs[-1]['term'] if self.logs else None
    }
    msg = pickle.dumps(msg)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

            tcp.settimeout(0.5)

            # Connects to server destination
            tcp.connect(('', value["port"]))

            print(f'Requesting vote to node {node}: {value["name"]}')

            # Send message
            tcp.sendall(msg)

            # Receives data from the server
            reply = tcp.recv(1024)

            if not reply:
                print("Reply not recieved")
                return reply

            reply = pickle.loads(reply)
            return reply

    except TimeoutError as te:
        print(te.args[0])
        tcp.close()

    except Exception as e:
        tcp.close()
        print(e)

    except KeyboardInterrupt:
        raise SystemExit()
    tcp.close()
    return {'candidate_id':'error'}

def send_message(msg, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
        # Connects to server destination
        tcp.connect(('', port))
        # Send message
        tcp.sendall(msg)


def persist_state(self):

    data = {
        '_current_term': self._current_term,   # Latest term server has seen
        '_voted_for': self._voted_for
    }

    with open(self._name + '.state', 'w') as file:
        json.dump(data, file)

def load_configuration(self):
    with open(self._name + '.state', 'r') as file:
        data = json.load(file)
        self._current_term = data['_current_term']
        self._voted_for = data['_voted_for']
    print('Configuration loaded: ', data)

def load_logs(self):
    with open(self._name + '.log', 'r') as f:
        for line in f:
            self.logs.append(json.loads(line))
    self.ack_logs = [len(nodos)] * len(self.logs)
    self._commit_index = len(self.logs)
    print('Logs loaded: ', self.logs)
