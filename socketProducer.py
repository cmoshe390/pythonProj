import queue


class SocketProducer:

    def __init__(self, _server):
        self._server = _server
        self._queue = queue.Queue()

    @staticmethod
    def could_publish_messages():
        return True

    def insert_val_to_queue(self, val):
        self._queue.put(val)
        self.publish_messages()

    def publish_messages(self):
        if not self._queue.empty():
            msg = self._queue.get()
            all_sockets_for_clients = self._server.get_the_all_sockets_for_clients()
            for sock in all_sockets_for_clients:
                sock.send_with_len(msg)

    def run(self):
        pass
