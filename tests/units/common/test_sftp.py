from unittest.mock import patch

import pytest
from common.sftp_service import DirectoryNotFoundException, SFTPService
from paramiko import SFTPClient


def test_connect():
    def initiate_sftp_service():
        with SFTPService() as sftp:
            return sftp

    assert initiate_sftp_service() is not None


@patch.object(SFTPClient, attribute="stat", side_effect=FileNotFoundError)
def test_connect_should_crash(connection_mock, *args):
    def initiate_sftp_service():
        with SFTPService():
            pass

    pytest.raises(DirectoryNotFoundException, initiate_sftp_service)


def test_error_raise():
    with pytest.raises(Exception, match="Test"), SFTPService():
        raise Exception("Test")
