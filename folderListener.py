import os
import time
import shutil
from os.path import isfile, join
import threading


class Listener:
    """
        Listener is a class which listen
        to a folder try to recognize
        there a files every 5 sec.
    """

    def __init__(self, _dir, _backup_dir, _publishers, _handler_files):
        self._dir = _dir
        self._backup_dir = _backup_dir
        self._publishers = _publishers
        self._files = []
        self._handler_files = _handler_files  # a function which will extract files from a zip folder

    def listener_to_files(self):
        files = []
        for file in os.listdir(self._dir):
            if isfile(join(self._backup_dir, file)):
                os.remove(join(self._backup_dir, file))
            files.append(file)
            shutil.move(f'{self._dir}\{file}', self._backup_dir)
        return files

    def run(self):
        num_of_publishers = len(self._publishers)
        non_stop = True
        count = 0
        while non_stop:
            time.sleep(3)
            self._files = self.listener_to_files()
            while not self._files == []:
                file_name = self._files.pop()
                file_name = self._handler_files(file_name)
                threading.Thread(target=self._publishers[count % num_of_publishers].publish_messages,
                                 args=(file_name,)).start()
                count += 1
            time.sleep(2)
            print(f' there is not yet a new file in the {self._dir} folder')
