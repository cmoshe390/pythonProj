import socket
from socketWrapper import *

IP = "0.0.0.0"
PORT = 2020
chunk_size = 1024
max_of_clients = 5


class Server:

    def __init__(self):
        self._all_sockets_for_clients = []
        self._server_socket = None
        self._sock = None

    def open_socket(self):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((IP, PORT))
        self._server_socket.listen()
        print("the server socket is up and running")

    def close_socket(self):
        self._server_socket.close()
        print("the server socket is close.")

    def listening_to_clients(self):
        (receiver_socket, receiver_address) = self._server_socket.accept()
        self._sock = SocketWrapper(receiver_socket)
        self._all_sockets_for_clients.append(self._sock)
        print(f"a new client connected")

    def get_the_all_sockets_for_clients(self):
        return self._all_sockets_for_clients

    def run(self):
        self.open_socket()

        num_of_clients = 0
        _flag = True
        while _flag:
            self.listening_to_clients()
            num_of_clients += 1
            if num_of_clients == max_of_clients:
                _flag = False

        self.close_socket()
