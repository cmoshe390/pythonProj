import socket
from socketWrapper import *
import json
from handleMessages import HandleMessages

IP = "127.0.0.1"
PORT = 2020


class SocketConsumer:

    def __init__(self, _id_consumer):
        self._id_consumer = _id_consumer
        self._receiver_socket = None
        self._socket_wrapper = None
        self._handle_messages = HandleMessages(_id_consumer)

        self.open_socket()

    def open_socket(self):
        self._receiver_socket = socket.socket()
        _flag = True
        while _flag:
            try:
                self._receiver_socket.connect((IP, PORT))
                print(f"the sender socket is up and connected to receiver{self._id_consumer}.")
                _flag = False
            except Exception:
                print("the sender socket is not up still.")
        self._socket_wrapper = SocketWrapper(self._receiver_socket)

    def close_socket(self):
        self._receiver_socket.close()

    def consume_data(self):
        data = json.loads(self._socket_wrapper.read_with_len())
        self._handle_messages.handler(data)

    def run(self):
        non_stop = True
        # trying to get an images files from the sender all the time.
        while non_stop:
            self.consume_data()
        self.close_socket()
