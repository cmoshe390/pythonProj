import os
import base64
import json
from datetime import datetime


class PublishFromFile:

    def __init__(self, _id_producer, _backup_dir, _chunk_size, _producer):
        self._id_producer = _id_producer
        self._backup_dir = _backup_dir
        self._chunk_size = _chunk_size
        self._producer = _producer

    def publish(self, file_name):
        file_path = os.path.join(self._backup_dir, file_name)
        with open(file_path, mode='br') as file:
            num_of_chunks = self.get_num_of_chunks(file_path)
            self.publish_whole_file(file, file_name, num_of_chunks)

    def get_num_of_chunks(self, file_name):
        length = os.path.getsize(file_name)
        num_of_chunks = int(length / self._chunk_size) + 1
        # send to the Receiver the number of the chunks for checking if all the messages were came.
        return num_of_chunks

    def publish_whole_file(self, file, file_name, num_of_chunks):
        _flag = True
        _id = 0
        t = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        while _flag:
            temp = file.read(self._chunk_size)
            _dict = self.make_temporary_dict(temp, _id, num_of_chunks, file_name)
            if temp == b'':
                print(f' producer{self._id_producer} start publish: {file_name}')
                print(f'{t}\n')
                print(f' producer{self._id_producer} sent: {file_name}')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                _flag = False
            else:
                try:
                    self._producer.publish_messages(json.dumps(_dict))
                except Exception as e:
                    print(e)
            _id += 1

    @staticmethod
    def make_temporary_dict(temp, _id, num_of_chunks, file_name):
        _dict = {}
        encoded = base64.b64encode(temp)
        _dict["id"] = _id
        # Decode the encoded bytes to str.
        _dict["chunk"] = encoded.decode()
        _dict["num_of_chunks"] = num_of_chunks
        _dict["file_name"] = file_name
        return _dict

    def get_id_producer(self):
        return self._id_producer
