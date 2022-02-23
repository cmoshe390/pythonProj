from collections import defaultdict
from datetime import datetime


class WriteToFile:

    def __init__(self, _id_consumer):
        self._id_consumer = _id_consumer
        # a dict which handle the counter of the all chunks which have been consume for the specific file name.
        self.dict_counters = defaultdict(int)

    def write_chunks_to_new_file(self, file_name, num_of_chunks, _list):
        # if the first chunks of the file were arrived.
        with open(f'receiver{self._id_consumer}New{file_name}', mode='ab') as file:
            data = b''
            for i in range(len(_list)):
                data += _list[i]
            self.dict_counters[file_name] += len(_list)
            file.write(data)

        # if all the chunks of the specific file name have been consumed.
        if self.dict_counters[file_name] == num_of_chunks:
            self.dict_counters[file_name] = 0
            print(f' a new file: new{file_name} was arrived to the consumer- {self._id_consumer}')
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
