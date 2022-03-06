from copy import deepcopy
import random
import time
import socket
import signal
import sys
import pickle
import json
import my_exceptions
import pickle

from settings import *


def create_append_msg(self, type, node):
    logs = self.logs[self.next_index[node]: self.next_index[node] + 4] # sending blocks of 4 entries
    msg = {
        'type': type,
        'term': self._current_term,
        'leader_id': self._name,
        'leader_port': self.PORT,  # AppendEntries requests include the network address of the leader
        'prev_log_index': self.next_index[node] - 1,
        'prev_log_term': self.logs[self.next_index[node] - 1]['term'] if self.next_index[node] > 0 else None,
        'change': logs,
        'commit_index': self._commit_index,
        'from': nodos[self._id]['port']
    }
    return pickle.dumps(msg)

def replicate_logEntry(self, msg):
    print('Recieved msg from client')
    msg.update({'from':nodos[self._id]['port']})
    # Only the leader handles it
    if self._state == 'Leader':  # This process is called Log Replication
        msg_to_send = dict(msg['change'])
        msg_to_send.pop('group_public_key', None)
        msg_to_send.pop('private_key_encrypted', None)

        send_message(msg_to_send, nodos[self._id]['port'], interface['port'])
        # change goes to the leader
        print('Leader append log: ', msg['change'])
        log = {'term': self._current_term}
        log.update(msg['change'])
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
        port_forward(self, msg)

def port_forward(self, msg):
    next_node_id = (int(self._id))%(len(nodos)) + 1 
    while(str(next_node_id)!= self._id):
        next_port = nodos[str(next_node_id)]['port']

        ping = send_ping( nodos[self._id]['port'], next_port)

        if ping and next_port not in self._broken_links:
            send_message(msg, nodos[self._id]['port'], nodos[str(next_node_id)]['port'])
            return
        next_node_id = (next_node_id)%(len(nodos)) + 1
    
    send_message({'Command':msg['change']['type']+" "+msg['change']['group_id'], 'Status':'Command failed! Not enough servers up for commiting the log.'}, 123, interface['port'])

def send_ping(sender_port, port):

    try:            
        ping = pickle.dumps({'type':'ping', 'from':sender_port })
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

            tcp.connect(('', port))

            print(f'Sending ping to Port: {port}')
            tcp.sendall(ping)
            reply = tcp.recv(4098)
            if reply:
                tcp.close()
                return True
    except Exception as e:
        print(e,'send_ping')
    return False


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
            self._voted_for = None
            persist_state(self)
        self._state = "Follower"
        self._election_timeout = self.get_election_timeout()
        self.config_timeout()
        #  or \
        # (msg['term'] == self._current_term and (not msg['prev_log_term']) or (len(self.logs) > msg['prev_log_index']) \
        # and msg['prev_log_term'] == self.logs[msg['prev_log_index']]['term']):
        print('Message: ', msg) if DEBUGGING_ON else None
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
            self.commit(self._commit_index + len(msg['change']))

            print('Append entry successful')
            print('Log: ', self.logs) if DEBUGGING_ON else None
            
    reply = pickle.dumps(ack_msg)
    conn.sendall(reply)


# Remote procedure call
def request_vote(self, node, value):
    '''
    This method is udes by a candidate requesting votes from everyone in its cluster
    '''
    msg = {
        'type': 'req_vote',
        'term': self._current_term,
        'candidate_id': self._name,
        'last_log_index': len(self.logs) - 1,
        'last_log_term': self.logs[-1]['term'] if self.logs else None,
        'from':nodos[self._id]['port']
    }
    msg = pickle.dumps(msg)
    error = ""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

            tcp.settimeout(0.5)

            # Connects to server destination
            tcp.connect(('', value["port"]))

            print(f'Requesting vote to node {node}: {value["name"]}')

            # Send message
            tcp.sendall(msg)

            # Receives data from the server
            reply = tcp.recv(4098)

            if not reply:
                print("Reply not received")
                return reply

            reply = pickle.loads(reply)
            return reply
    except TimeoutError as te:
        print(te.args[0])
        error = te.args[0]
        tcp.close()

    except Exception as e:
        tcp.close()
        error = e
        print(e,'request_vote')

    except KeyboardInterrupt:
        raise SystemExit()
    tcp.close()
    return {'candidate_id':error}

def send_message(msg, sender_port, port):
    if port!=interface['port']:
        msg.update({'from': sender_port})
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
            # Connects to server destination
            tcp.connect(('', port))
            # Send message
            tcp.sendall(pickle.dumps(msg))
            tcp.close()
    except Exception as e:
        print(e,'send_message')

def id_to_name(client_id):
    return chr(ord(client_id)-ord('1')+ord('a'))

def persist_state(self):

    data = {
        '_current_term': self._current_term,   # Latest term server has seen
        '_voted_for': self._voted_for,
        '_broken_links': self._broken_links
    }

    with open(self._name + '.state', 'w') as file:
        json.dump(data, file)

def load_state(self):
    with open(self._name + '.state', 'r') as file:
        data = json.load(file)
        self._current_term = data['_current_term']
        self._voted_for = data['_voted_for']
        self._broken_links = data['_broken_links']
    print('Configuration loaded: ', data)

def load_logs(self):
    try:
        with open(self._name + '.log', 'rb') as f:
            self.logs = pickle.load(f)
        self.ack_logs = [len(nodos)] * len(self.logs)
        self._commit_index = len(self.logs) - 1
        print('Logs loaded: ', self.logs) if DEBUGGING_ON else None
    except EOFError:
        self.logs = list() 

def log_with_gid(self, gid):
    for log in reversed(self.logs):
        if log['group_id'] == gid:
            return deepcopy(log)

