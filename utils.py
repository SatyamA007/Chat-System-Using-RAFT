import random
import time
import socket
import signal
import sys
import json
import my_exceptions
import pickle

from settings import *

def interface_receive_message(self, msg):
    print('Recieved msg from interface')
    # Only the leader handles it
    if self._state == 'Leader':  # This process is called Log Replication
        # change goes to the leader

        print('Leader append log: ', msg['change'])
        self._log = (msg['change'])  # Each change is added as an entry in the nodes's log
        print(self._log)
        self._ack_log += 1

    # This log entry is currently uncommitted so it won't update the node's value.

    # To commit the entry the node first replicates it to the follower nodes...
    # Then the leader waits until a majority of nodes have written the entry.
    # The entry is now committed on the leader node and the node state is "X"
    # The leader then notifies the followers that the entry is committed.
    # The cluster has now come to consensus about the system state.

    # If a follower receives a message from a client the it must redirect to the leader
    else:
        redirect_to_leader(self, msg)

def redirect_to_leader(self, msg):
    next_node_port = (self.PORT - 5000)%(len(nodos)) + 5001
    send_message(msg, next_node_port)
    
def reply_append_entry(self, msg, conn):
    """
    An entry is committed once a majority of followers acknowledge it...
    :param append_entry_msg:
    :return:
    """
    # TODO: Acknowledge message
    self._election_timeout = self.get_election_timeout()
    self.config_timeout()
    self._state = "Follower"
    self._log = (msg['change'])

    ack_msg = {
        'client_id': self._name,
        'term': self._current_term,
        'type': 'ack_append_entry',
        'change': self._log
    }

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
        'last_log_index': self._last_applied,
        'last_log_term': self._commit_index
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
            reply = tcp.recv(4098)

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
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
            # Connects to server destination
            tcp.connect(('', port))
            # Send message
            tcp.sendall(pickle.dumps(msg))
    except Exception as e:
        print(e)

def id_to_name(client_id):
    return chr(ord(client_id)-ord('1')+ord('a'))