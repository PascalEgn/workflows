import pytest
from airflow.models import DagModel


@pytest.fixture(scope="session")
def vcr_config():
    return {
        "ignore_localhost": True,
        "decode_compressed_response": True,
        "filter_headers": ("Authorization", "X-Amz-Date"),
        "record_mode": "once",
    }


@pytest.fixture
def dag_was_paused(dag):
    dag_model = DagModel.get_dagmodel(dag_id=dag.dag_id)
    return dag_model.is_paused
