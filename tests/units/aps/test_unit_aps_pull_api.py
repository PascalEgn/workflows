import json
from unittest import mock

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.mark.usefixtures("dagbag")
class TestUnitApsPullApi:
    def setup_method(self):
        self.dag_id = "aps_pull_api"
        self.dag = DagBag(dag_folder="dags/", include_examples=False).get_dag(
            self.dag_id
        )
        assert self.dag is not None, f"DAG {self.dag_id} failed to load"

    def test_prepare_trigger_conf(self):
        task = self.dag.get_task("prepare_trigger_conf")
        function_to_unit_test = task.python_callable

        # Mock dependencies
        # prepare_trigger_conf calls split_json(repo, key)
        # split_json expects repo.get_by_id(key) to return a file-like object with json data

        mock_repo = mock.MagicMock()
        mock_file = mock.MagicMock()

        # Prepare valid JSON content
        # split_json expects: json.loads(...)["data"] which is a list of articles
        json_content = json.dumps(
            {
                "data": [
                    {"id": "1", "title": "Article 1"},
                    {"id": "2", "title": "Article 2"},
                ]
            }
        ).encode("utf-8")

        mock_file.getvalue.return_value = json_content
        mock_repo.get_by_id.return_value = mock_file

        # Call the function
        result = function_to_unit_test(key="some_key", repo=mock_repo)

        # Check result
        # prepare_trigger_conf returns [{"article": json.dumps(item["article"])} ...]
        assert len(result) == 2
        assert json.loads(result[0]["article"]) == {"id": "1", "title": "Article 1"}
        assert json.loads(result[1]["article"]) == {"id": "2", "title": "Article 2"}

    def test_dag_structure(self):
        # Verify the trigger task exists in the DAG
        assert "aps_trigger_file_processing" in self.dag.task_ids

        task = self.dag.get_task("aps_trigger_file_processing")

        # Verify it is a mapped operator
        assert "MappedOperator" in str(type(task)) or task.is_mapped

        # Verify static configuration
        assert task.partial_kwargs["trigger_dag_id"] == "aps_process_file"
        assert task.partial_kwargs["reset_dag_run"] is True
