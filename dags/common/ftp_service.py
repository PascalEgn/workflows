import ftplib
import io
import logging
import os
import re
import traceback

from common.exceptions import DirectoryNotFoundException, NotConnectedException
from common.utils import append_not_excluded_files, walk_ftp

logger = logging.getLogger("airflow.task")


class FTPService:
    def __init__(
        self,
        host="localhost",
        username="airflow",
        password="airflow",
        port=2222,
        dir="/upload",
    ):
        self.connection = None
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.dir = dir

    def __connect(self):
        ftp = ftplib.FTP(self.host)
        ftp.login(self.username, self.password)
        try:
            ftp.dir(self.dir)
        except FileNotFoundError as e:
            raise DirectoryNotFoundException(
                "Remote directory doesn't exist. Abort connection."
            ) from e
        return ftp

    def __enter__(self):
        self.connection = self.__connect()
        return self

    def __exit__(self, exception_type, exception_value, tb):
        if self.connection:
            self.connection.close()
        if exception_type is not None:
            formed_exception = traceback.format_exception_only(
                exception_type, exception_value
            )
            logger.error(
                "An error occurred while exiting FTPService: %s",
                formed_exception,
            )
            return False
        return True

    def list_files(self, excluded_directories=None):
        try:
            file_names = []
            filtered_files = []
            walk_ftp(ftp=self.connection, remotedir=self.dir, paths=file_names)
            for file_name in file_names:
                append_not_excluded_files(
                    re.sub(self.dir + "/", "", file_name),
                    excluded_directories,
                    filtered_files,
                )
            return filtered_files
        except AttributeError as e:
            raise NotConnectedException from e

    def get_file(self, file):
        try:
            file_contents = io.BytesIO()
            file_path = os.path.join(self.dir, file)
            logger.info("Downloading file %s", file_path)
            self.connection.retrbinary(f"RETR {file_path}", file_contents.write)
            return file_contents
        except AttributeError as e:
            raise NotConnectedException from e
