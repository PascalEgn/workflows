from io import BytesIO

import pytest
from common.repository import IRepository


@pytest.fixture
def repo():
    return IRepository()


def test_find_all(repo):
    pytest.raises(NotImplementedError, repo.find_all)


def test_test_find_by_id(repo):
    pytest.raises(NotImplementedError, repo.get_by_id, id="")


def test_save(repo):
    pytest.raises(NotImplementedError, repo.save, filename="", obj=BytesIO())


def test_delete_all(repo):
    pytest.raises(NotImplementedError, repo.delete_all)
