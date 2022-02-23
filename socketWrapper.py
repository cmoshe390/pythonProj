import struct


class SocketWrapper:

    def __init__(self, _socket_obj):
        self._socket_obj = _socket_obj

    def send_socket(self, data):
        # if there is no information, the empty data will be sent with the socket as it is.
        if len(data) == 0:
            self._socket_obj.send(data)
        else:
            index = 0
            while index < len(data):
                index += self._socket_obj.send(data[index:])

    def send_with_len(self, data):
        self.send_socket(struct.pack('>I', len(data)))
        if type(data) is bytes:
            self.send_socket(data)
        else:
            self.send_socket(data.encode('utf-8'))

    def send_int(self, int_num):
        self.send_socket(str(int_num).encode('utf-8'))

    def read_socket(self, should_read, output_type='str'):
        buf = bytearray()
        total_read = 0
        while total_read < should_read:
            left_to_read = should_read - total_read
            curr_recv = self._socket_obj.recv(left_to_read)
            if curr_recv == b'':
                return buf
            total_read += len(curr_recv)
            buf += bytearray(curr_recv)
        if output_type == 'str':
            return buf.decode('utf-8')
        return buf

    def read_with_len(self, output_type='str'):
        length_bytes = self.read_socket(4, output_type='bytes')
        if len(length_bytes):
            length = struct.unpack('>I', length_bytes)[0]
            return self.read_socket(length, output_type)

    def read_int(self):
        num_bytes = self.read_socket(4, output_type='bytes')
        if len(num_bytes):
            return int(num_bytes.decode())
