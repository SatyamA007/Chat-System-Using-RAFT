import json
import pickle
import signal
import socket
import time


from settings import *
from utils import send_message, send_ping

class Client:

    def __init__(self, port):
        self.PORT = int(port)
        
        signal.signal(signal.SIGALRM, self.get_command)
        self.config_timeout()
        self.start_server()
            
    def start_server(self):

        print('creating socket')

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
            # Join socket to host and port
            tcp.bind(('', self.PORT))  #Receive messages from any host

            # Enable the server to accept 5 connections
            tcp.listen(5)

            while True:

                try:
                    conn, address = tcp.accept()

                    with conn:
                        print('Connected by', address)

                        # Receive customer data
                        msg = conn.recv(4098)

                        # If something goes wrong with the data, exit the loop
                        if not msg:
                            print('Nothing recieved')
                            conn.close()
                            return

                        msg = pickle.loads(msg)

                        # Print received data
                        print('Message received: ', msg)

                except Exception:
                    self.get_command()
                    self.config_timeout()

    def config_timeout(self):
        signal.alarm(5)

    def get_command(self):
        while True:
            client = input("Client for sending the command to (1-5): ").strip()
            if client not in nodos.keys() or self.check_invalid_command(client):
                print("Wrong Input!! Try agian...")
                continue
            else:
                break

    def check_invalid_command(self, client:str):
        command = input('''Please issue a command from the following: \n1. createGroup <group id> <candidate_id>(s)\n\
2. add <group id> <client id>\n3. kick <group id> <client id>\n4. writeMessage <group id> <message>\n\
5. printGroup <group id>\n6. failLink <src> <dest>(s)\n7. fixLink <src> <dest>(s)\n8. failProcess\n''')
        command = command.strip().split(' ')
        command[0] = command[0].lower()

        if command[0] =="creategroup" and (len(command)==2 or len(command)>2 and set(command[2:]).issubset(set(nodos.keys())) ):
            client_ids = [client] 
            if len(command)>2: 
                client_ids.extend(command[2:])
            msg = {'type': 'create_group', 'group_id': command[1], 'client_ids':list(set(client_ids)), 'from': interface['port']}
            
        elif command[0] =="add" and len(command)==3 and command[2] in nodos.keys():
            msg = { 'type': 'add2group', 'group_id': command[1], 'node': command[2], 'from': interface['port']}

        elif command[0] =="kick" and len(command)==3 and command[2] in nodos.keys():
            msg = { 'type': 'kick', 'group_id': command[1], 'node': command[2], 'from': interface['port'] }
            
        elif command[0] =="writemessage" and len(command)>=3:
            msg = { 'type': 'write_message', 'group_id': command[1], 'message': " ".join(command[2:]), 'from': interface['port'] }
            
        elif command[0] =="printgroup" and len(command)==2:
            msg = { 'type': 'print_group', 'group_id': command[1], 'from': interface['port'] }
            
        elif command[0] =="faillink" and len(command)>=3 and command[1] in nodos.keys() and set(command[2:]) <= set(nodos.keys()):
            if command[1]!= client:
                print("Error: <src> must be equal to the receiving client")
                return True
            msg = { 'type': 'fail_link', 'src': command[1], 'dst': command[2:], 'from': interface['port'] }

        elif command[0] =="fixlink" and len(command)>=3 and command[1] in nodos.keys() and set(command[2:]) <= set(nodos.keys()):
            if command[1]!= client:
                print("Error: <src> must be equal to the receiving client")
                return True
            msg = { 'type': 'fix_link', 'src': command[1], 'dst': command[2:], 'from': interface['port'] }

        elif command[0] =="failprocess":
            msg = { 'type': 'fail_process', 'from': interface['port']}

        else:
            return True
        
        attempt = 0
        while( attempt< MAX_RETRIES):
            ping = send_ping(interface['port'], nodos[client]['port'])
            if ping:
                print(f"Ping succesful, client alive")
                send_message(msg, interface['port'], nodos[client]['port'])
                return False  
            print(f"Request {attempt+1} timeout....retrying")
            time.sleep(1)
            attempt+=1
        
        print(f"Request failed....Client not connected!!")
            

if __name__== "__main__":
    interface = Client(interface['port'])
