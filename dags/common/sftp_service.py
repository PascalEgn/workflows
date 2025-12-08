import io
import logging
import os
import re
import traceback

import paramiko
from common.exceptions import DirectoryNotFoundException, NotConnectedException
from common.utils import append_not_excluded_files, walk_sftp

logger = logging.getLogger("airflow.task")


class SFTPService:
    def __init__(
        self,
        host=None,
        username=None,
        password=None,
        port=None,
        dir=None,
        private_key_content=None,
    ):
        if host is None:
            host = os.getenv("COMMON_SFTP_HOST", "localhost")
        if username is None:
            username = os.getenv("COMMON_SFTP_USERNAME", "airflow")
        if password is None:
            password = os.getenv("COMMON_SFTP_PASSWORD", "airflow")
        if port is None:
            port = int(os.getenv("COMMON_SFTP_PORT", 2222))
        if dir is None:
            dir = os.getenv("COMMON_SFTP_DIR", "/upload")
        self.connection = None
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.private_key_content = private_key_content
        self.dir = dir

    def __connect(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = {
            "banner_timeout": 200,
            "hostname": self.host,
            "username": self.username,
            "port": self.port,
        }

        if self.private_key_content:
            private_key_file = io.StringIO(self.private_key_content)
            connect_kwargs["pkey"] = paramiko.RSAKey.from_private_key(private_key_file)
        elif self.password:
            connect_kwargs["password"] = self.password
        else:
            raise ValueError(
                "No authentication method provided (private key or password)"
            )

        client.connect(**connect_kwargs)

        connection = client.open_sftp()
        try:
            connection.stat(self.dir)
        except FileNotFoundError as e:
            raise DirectoryNotFoundException(
                "Remote directory doesn't exist. Abort connection."
            ) from e
        return connection

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
                "An error occurred while exiting SFTPService. Error: %s",
                str(formed_exception),
            )
            return False
        return True

    def list_files(self, excluded_directories=None):
        try:
            file_names = []
            filtered_files = []
            walk_sftp(sftp=self.connection, remotedir=self.dir, paths=file_names)
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
            file_ = self.connection.open(os.path.join(self.dir, file))
            file_.prefetch()
            return file_
        except AttributeError as e:
            raise NotConnectedException from e
