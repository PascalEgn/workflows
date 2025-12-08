import sys
import unittest
import warnings
from pathlib import Path

import airflow
import pytest


def pytest_configure():
    warnings.filterwarnings(
        "ignore",
        message=r"Implicit imports.*might be removed.*",
        category=DeprecationWarning,
        module=r"idutils|.*parser",
    )


print("Initilized environment with", airflow.__name__)
dags_path = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(dags_path))


@pytest.fixture(scope="session")
def test_case_instance():
    return unittest.TestCase()


@pytest.fixture(scope="session")
def assertListEqual(test_case_instance):
    return lambda first, second: test_case_instance.assertCountEqual(first, second)  # noqa: PT009


@pytest.fixture(scope="session")
def vcr_config():
    return {
        "ignore_localhost": True,
        "decode_compressed_response": True,
        "filter_headers": ("Authorization", "X-Amz-Date"),
        "record_mode": "once",
    }
