from zipfile import ZipFile
import os


class ExtractFiles:

    def __init__(self):
        self.ends_with = '.zip'
        self._backup_dir = r"C:\dev\integratedSystem\all_images"

    def handler_files(self, file_name):
        if file_name.endswith(self.ends_with):
            return self.extract_zips(file_name)
        else:
            return file_name

    def extract_zips(self, file_name):
        file_path = os.path.join(self._backup_dir, file_name)
        with ZipFile(file_path, 'r') as zip_ref:
            # Extract all the contents of zip file in current directory
            return zip_ref.namelist()[0]
