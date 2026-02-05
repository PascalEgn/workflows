import os

from common.sftp_service import SFTPService


class ElsevierSFTPService(SFTPService):
    def __init__(self):
        super().__init__(
            host=os.getenv("ELSEVIER_SFTP_HOST", "localhost"),
            username=os.getenv("ELSEVIER_SFTP_USERNAME", "airflow"),
            password=os.getenv("ELSEVIER_SFTP_PASSWORD", "airflow"),
            port=int(os.getenv("ELSEVIER_SFTP_PORT", "2222")),
            dir=os.getenv("ELSEVIER_SFTP_DIR", "upload/elsevier"),
        )
