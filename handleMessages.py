import base64
from writeToFile import WriteToFile
from collections import defaultdict


class HandleMessages:

    def __init__(self, _id_consumer):
        self.writer_to_file = WriteToFile(_id_consumer=_id_consumer)

        # this dict holder a list with the data for every key (file_name) inside her.
        self._dict_of_files = defaultdict(list)

        # this dict holder a counter of iterations for every key (file_name) inside her.
        self._dict_count_iterations = defaultdict(int)

        # the size of the max chunks which will be stored in memory.
        self._window_size = 20

    def could_handle_msg(self, msg):
        file_name = msg["file_name"]
        _id = msg["id"]
        if _id < self._window_size * (self._dict_count_iterations[file_name] + 1):
            return True
        return False

    def handler(self, msg):
        self.update_dict_of_files(msg)

    def update_dict_of_files(self, msg):
        _id, data_in_bytes, num_of_chunks, file_name = self.get_data_from_dict(msg)

        # add the msg to the list of his file name.
        self._dict_of_files[file_name].append(msg)

        # msg_list is part of the messages list of specific file.
        msg_list = self._dict_of_files[file_name]

        # list ids saves the all ids of the messages in the current window size.
        _list_ids = []
        for msg in self._dict_of_files[file_name]:
            _list_ids.append(msg["id"])

        is_finish = False
        # if the last group of the chunks' file were consumed.
        if len(msg_list) == (num_of_chunks % self._window_size) and num_of_chunks - 1 in _list_ids:
            is_finish = True
        if len(msg_list) == self._window_size and num_of_chunks - 1 in _list_ids:
            is_finish = True

        # if the window_size of chunks of the file were arrived- they will be written to the file.
        if len(msg_list) == self._window_size or is_finish:
            self.write_chunks_to_file_from_list(msg_list, is_finish)
            self._dict_of_files[file_name] = []
            self._dict_count_iterations[file_name] += 1

    def write_chunks_to_file_from_list(self, chunks_list, is_finish):
        arranged_list = []
        for chunk in chunks_list:
            _id, data_in_bytes, num_of_chunks, file_name = self.get_data_from_dict(chunk)
            arranged_list.insert(_id - self._window_size * self._dict_count_iterations[file_name], data_in_bytes)
        self.writer_to_file.write_chunks_to_new_file(file_name, num_of_chunks, arranged_list)

        # if the last group of the chunks' file were consumed.
        if is_finish:
            self._dict_count_iterations[file_name] = -1

    @staticmethod
    def get_data_from_dict(_dict):
        _id = _dict["id"]
        data_in_str = _dict["chunk"]
        num_of_chunks = _dict["num_of_chunks"]
        file_name = _dict["file_name"]
        data_in_bytes = base64.b64decode(data_in_str)
        return _id, data_in_bytes, num_of_chunks, file_name
