import os
import time
import zipfile
import shutil
import logging


class DataExecutor:
    def __init__(self, main_path, raw_relative_path, processing_relative_path):
        self.raw_relative_path = raw_relative_path
        self.main_path = main_path
        self.raw_relative_path = raw_relative_path
        self.processing_relative_path = processing_relative_path

    def copy_directory(self, source_dir, destination_dir):
        try:
            # shutil.copytree(source_dir, destination_dir)
            os.makedirs(destination_dir, exist_ok=True)
            items = [item for item in os.listdir(source_dir) if not item.endswith('_Sync')]
            for item in items:
                source_item = os.path.join(source_dir, item)
                destination_item = os.path.join(destination_dir, item)
                if os.path.isdir(source_item):
                    self.copy_directory(source_item, destination_item)
                else:
                    shutil.copy2(source_item, destination_item)

            logging.info(f"Directory '{source_dir}' successfully copied to '{destination_dir}'")
        except FileExistsError:
            logging.error(f"Directory '{destination_dir}' already exists. Copy operation aborted.")

    @staticmethod
    def unpacking_zip(dir_path):
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                if file.endswith('.zip'):
                    zip_file_path = os.path.join(root, file)
                    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                        zip_ref.extractall(os.path.splitext(zip_file_path)[0])
                        logging.info(f"Extracted contents of '{file}' to '{os.path.splitext(zip_file_path)[0]}'")
                    os.remove(zip_file_path)
                    logging.info(f"Deleted '{file}'")

    def call_all_functions(self):
        start_time = time.time()
        self.copy_directory(self.main_path + self.raw_relative_path, self.main_path + self.processing_relative_path)
        self.unpacking_zip(self.main_path + self.processing_relative_path)
        logging.info(f"Total time of moving and unzipping data: {round(((time.time() - start_time) / 60), 2)} min")


def delete_empty_files(root_dir, extension):
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(extension) and os.path.getsize(os.path.join(root, file)) == 0:
                os.remove(os.path.join(root, file))
                print(f"Deleted empty file: {os.path.join(root, file)}")
