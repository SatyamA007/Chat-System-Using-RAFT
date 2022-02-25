import json
import signal
import socket
import sys
import pickle


class Client:

    def __init__(self, port):
        self.PORT = int(port)

        self.config_timeout()
        self.start_server()

    def send_change(self):
        # Create a tcp socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

            port = int(input("Leader's port:"))

            # Receive message to be sent
            msg_value = input("Enter the message to be sent:")

            # connects no server
            tcp.connect(('', port))

            msg = {
                'type': 'client',
                'change': msg_value
            }
            msg = pickle.dumps(msg)
            msg = msg

            # Sends messge
            tcp.sendall(msg)
            msg = tcp.recv(1024)
            if not msg:
                print('Nothing recieved')
                tcp.close()
                return

            msg = msg
            msg = pickle.loads(msg)

            # Print received data
            print('Msg recieved: ', msg)
            
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
                        msg = conn.recv(1024)

                        # If something goes wrong with the data, exit the loop
                        if not msg:
                            print('Nothing recieved')
                            conn.close()
                            return

                        msg = msg
                        msg = pickle.loads(msg)

                        # Print received data
                        print('Msg recieved: ', msg)

                except Exception:
                    self.send_change()
                    self.config_timeout()

    def config_timeout(self):
        signal.signal(signal.SIGALRM, self.send_change)
        signal.alarm(10)

if __name__== "__main__":

    port = sys.argv[1]
    client = Client(port)