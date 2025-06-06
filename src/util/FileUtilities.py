import os
from datetime import datetime, timezone
import pathlib
import hashlib
from fastapi import HTTPException
from src.conf.Configurations import logger
import shutil
from src.util.FileMetadataDatabaseUtility import FileMetadataDatabaseUtility
import re

class FileUtilities:
    """
    This class provides utility functions for file operations such as metadata extraction, file saving, and version management.
    It includes methods to compute file hashes, search for files, and manage file versions.
    """
    def __init__(self):
        """
        This function initializes the FileUtilities class.
        """
        self.logger = logger

    # This function converts the time from epoch to datetime format
    def __time_convert(self, a_time):
        """
        Convert the time from epoch to datetime

        :param a_time: The time in epoch format
        :return: The time in datetime format
        """
        return datetime.fromtimestamp(a_time).strftime('%Y-%m-%d %H:%M:%S')


    # This function formats the size from bytes to KB
    def __size_format(self, size):
        """
        Convert the size from bytes to KB

        :param size: The size in bytes
        :return: The size in KB
        """
        new_form = format(size / 1024, ".2f")
        return str(new_form)

    # This function computes the hash of a file using the specified algorithm
    def __compute_hash(self, file_path, algorithm='md5'):
        """
        Compute the hash of a file using the given algorithm

        :param file_path: The path of the file
        :param algorithm: The hashing algorithm to use
        :return: The hash of the file
        """
        # Create a hash object
        if algorithm == 'md5':
            hasher = hashlib.md5()
        elif algorithm == 'sha1':
            hasher = hashlib.sha1()
        else:
            hasher = hashlib.sha256()

        # Open the file in binary read mode
        with open(file_path, 'rb') as file:
            # Read and update hash in chunks to save memory
            for chunk in iter(lambda: file.read(4096), b""):
                hasher.update(chunk)

        return hasher.hexdigest()

    # This function searches for a file in the given folder path
    def __search_file(self, file_name, folder_path):
        """
        Search for a file in the given folder path

        :param file_name: The name of the file to search for
        :param folder_path: The folder path to search in
        :return: A list of file names
        """
        names = []

        files = os.listdir(folder_path)

        try:
            for name in files:
                if (pathlib.Path(name).stem == file_name) | (name == file_name):
                    names.append(name)

            return names

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred while searching for the file in directory: {e}")

    # This function gets the metadata of a file
    def get_metadata(self, file_name, folder_path):
        """
        Get the metadata of a file

        :param file_name: The name of the file to get the metadata of
        :param folder_path: The folder path to search in
        :return: A dictionary with the metadata of the file
        """
        try:
            # Create the file path using os.path.join
            filepath = f'{folder_path}{os.sep}{file_name}'
            stats = os.stat(filepath)

            # Get file metadata
            dict_info = {
                'file_name': file_name,
                'source_path': folder_path,
                'destination_path': os.path.abspath(filepath),
                'file_type': pathlib.Path(filepath).suffix[1:],
                'file_size': self.__size_format(stats.st_size),
                'file_creation_date': self.__time_convert(stats.st_ctime),
                'file_modified_date': self.__time_convert(stats.st_mtime),
                'file_hash': self.__compute_hash(filepath),
                'created_on': datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'created_by': 'SYSTEM',
                'version_file': 0
            }

            return dict_info
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred while getting the metadata: {e}")

    # This function gets the created and modified dates of a file
    def __get_created_and_modified_dates(self, name, folder_path):
        """
        Get the created and modified dates of a file
        :param name: The name of the file to get the dates of
        :param folder_path: The folder path to search in
        :return: A dictionary with the created and modified dates of the file
        """

        try:

            file_path = os.path.join(folder_path, name)
            stats = os.stat(file_path)
            created_date = self.__time_convert(stats.st_ctime)
            modified_date = self.__time_convert(stats.st_mtime)

            return {
                'file_name': name,
                'file_creation_date': created_date,
                'file_modified_date': modified_date
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred while getting the dates: {e}")

    # This function checks if the file already exists in the destination folder
    def has_file_in_destination_folder(self, filename, folder_path):
        """
        Check if the file already exists in the destination folder

        :param filename: The name of the file to check
        :param folder_path: The folder path to check in
        :return: True if the file exists, False otherwise
        """
        names = self.__search_file(filename, folder_path)
        if len(names) == 0:
            return None
        else:
            return f"File {filename} already exists in {folder_path}. Please upload another file."

    # This function saves the file in the destination folder
    def save_file_in_destination_folder(self, file, base_path_upload):
        """
        Save the file in the destination folder
        :param file: The file to save
        :param base_path_upload: The base path to save the file
        :return: None
        """
        try:
            file_path = os.path.join(base_path_upload, file.name)

            with open(file_path, 'wb') as f:
                shutil.copyfileobj(file, f)
            self.logger.info(f"File saved successfully as {file_path}")

        except Exception as e:
            self.logger.error(f"Error saving file: {e}")
            raise HTTPException(status_code=500, detail="Error saving file")

    # This function lists all versioned files in the given folder path that match the file name
    def list_versioned_files(self, file_name, folder_path):
        """
        List all versioned files in the given folder path that match the file name
        :param file_name: The name of the file to search for
        :param folder_path: The folder path to search in
        :return: A list of dictionaries with the file names and their created and modified dates, and the category ID
        """
        # files = os.listdir(folder_path)
        file_records = FileMetadataDatabaseUtility().get_all_file_names_and_categories()

        file_dict = {name: category for name, category in file_records}

        if file_name in file_dict.keys():
            file_dict.pop(file_name)

        category = max(list(file_dict.values()), default=0) + 1

        names = []
        try:
            file_name_with_out_ext = file_name.split('.')[0]

            for name, cat in file_dict.items():
                if file_name_with_out_ext == name.split('.')[0]:
                    names.append(name)
                    category = cat

            if names:
                versioned_files = []

                for name in names:
                    file_info = self.__get_created_and_modified_dates(name, folder_path)

                    versioned_files.append(file_info)

                return versioned_files, category
            else:
                return None, category

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred while listing the versioned files: {e}")

    # This function removes the file from the given folder path
    def remove_file(self, file_name, folder_path):
        """
        Remove the file from the given folder path

        :param file_name: The name of the file to remove
        :param folder_path: The folder path to search in
        :return: None

        """
        try:
            file_path = os.path.join(folder_path, file_name)
            if os.path.exists(file_path):
                os.remove(file_path)
                self.logger.info(f"File {file_name} removed successfully.")
            else:
                self.logger.warning(f"File {file_name} does not exist in {folder_path}.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred while removing the file: {e}")

    # This function gets the created and modified dates of a file
    def get_dates(self, file_name, folder_path):
        """
        Get the created and modified dates of a file
        :param file_name: The name of the file to get the dates of
        :param folder_path: The folder path to search in
        :return: A dictionary with the created and modified dates of the file

        """

        res = self.__get_created_and_modified_dates(file_name, folder_path)

        return res

    # This function extracts the version number from the filename
    def extract_version(self, filename):

        """
        Extract the version number from the filename.

        :param filename: The name of the file from which to extract the version.
        :return: A string representing the version number, or '0' if no version is found.
        """

        # Remove the file extension and convert to lowercase
        name_without_ext = filename.rsplit('.', 1)[0].lower()

        # Use regex to find the version number in the filename
        match = re.search(r'(\d+(\.\d+)+)$', name_without_ext) or re.search(r'[\s.](\d+)$', name_without_ext)
        if match:
            return match.group(1)

        return '0'

if __name__ == "__main__":

    # Example usage
    file_utilities = FileUtilities()
    sample_folder_path = r"D:\DESTINATION_PATH"
    sample_file_name = "abc.pdf"

    # # Check if the file exists in the destination folder
    # file_exists = file_utilities.has_file_in_destination_folder(sample_file_name, sample_folder_path)
    #
    # print("File exists:", file_exists)
    #
    # # check versioned files
    # version_files = file_utilities.list_versioned_files(sample_file_name, sample_folder_path)
    # print("Versioned files:", version_files)
    #
    # version_files = file_utilities.list_versioned_files("abc.3.pdf", sample_folder_path)
    #
    # print("Versioned files:", version_files)

    # # get created and modified dates
    # file_dates = file_utilities.get_dates(sample_file_name, sample_folder_path)
    #
    # print("Created and modified dates:", file_dates)
    #
    # f = file_utilities.get_dates("abc.66.pdf", sample_folder_path)

    f = file_utilities.list_versioned_files("abc.66.pdf", sample_folder_path)

    print(f)
