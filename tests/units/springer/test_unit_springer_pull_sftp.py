import pytest
from airflow.models import DagBag


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.mark.usefixtures("dagbag")
class TestUnitSpringerPullSftp:
    def setup_method(self):
        self.dag_id = "springer_pull_sftp"
        self.dag = DagBag(dag_folder="dags/", include_examples=False).get_dag(
            self.dag_id
        )
        assert self.dag is not None, f"DAG {self.dag_id} failed to load"

    def test_dag_structure(self):
        assert "springer_trigger_file_processing" in self.dag.task_ids

        task = self.dag.get_task("springer_trigger_file_processing")
        assert "MappedOperator" in str(type(task)) or task.is_mapped
        assert task.partial_kwargs["trigger_dag_id"] == "springer_process_file"
        assert task.partial_kwargs["reset_dag_run"] is True
