import pytest
from common.pull_ftp import migrate_files
from elsevier.repository import ElsevierRepository
from elsevier.sftp_service import ElsevierSFTPService
from structlog import get_logger


@pytest.fixture
def elsevier_sftp():
    return ElsevierSFTPService()


@pytest.fixture
def elsevier_empty_repo():
    repo = ElsevierRepository()
    repo.delete_all()
    return repo


@pytest.fixture
def logger():
    return get_logger().bind(class_name="elsevier_pull_sftp")


@pytest.fixture
def migrated_files(elsevier_empty_repo, elsevier_sftp, logger):
    with elsevier_sftp as sftp:
        return migrate_files(
            ["CERNQ000000010011A.tar", "vtex00403986_a-2b_CLEANED.zip"],
            sftp,
            elsevier_empty_repo,
            logger,
            process_archives=False,
        )
