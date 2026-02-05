from unittest import mock

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.mark.usefixtures("dagbag")
class TestUnitHindawiPullApi:
    def setup_method(self):
        self.dag_id = "hindawi_pull_api"
        self.dag = DagBag(dag_folder="dags/", include_examples=False).get_dag(
            self.dag_id
        )
        assert self.dag is not None, f"DAG {self.dag_id} failed to load"

    def test_prepare_trigger_conf(self):
        task = self.dag.get_task("prepare_trigger_conf")
        function_to_unit_test = task.python_callable

        xml_content = b"""
<root>
    <ListRecords>
        <record>
            <header>1</header>
        </record>
        <record>
            <header>2</header>
        </record>
    </ListRecords>
</root>
        """

        mock_file = mock.MagicMock()
        mock_file.getvalue.return_value = xml_content

        mock_repo = mock.MagicMock()
        mock_repo.get_by_id.return_value = mock_file
        result = function_to_unit_test(key="some_key", repo=mock_repo)

        assert len(result) == 2
        assert "record" in result[0]
        assert "<header>1</header>" in result[0]["record"]
        assert "<header>2</header>" in result[1]["record"]

    def test_dag_structure(self):
        assert "hindawi_trigger_file_processing" in self.dag.task_ids
        task = self.dag.get_task("hindawi_trigger_file_processing")
        assert "MappedOperator" in str(type(task)) or task.is_mapped
        assert task.partial_kwargs["trigger_dag_id"] == "hindawi_file_processing"
        assert task.partial_kwargs["reset_dag_run"] is True
