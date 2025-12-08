from unittest import mock

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.mark.usefixtures("dagbag")
class TestUnitElsevierPullSftp:
    def setup_method(self):
        self.dag_id = "elsevier_pull_sftp"
        self.dag = DagBag(dag_folder="dags/", include_examples=False).get_dag(
            self.dag_id
        )
        assert self.dag is not None, f"DAG {self.dag_id} failed to load"

    def test_prepare_trigger_conf(self):
        task = self.dag.get_task("prepare_trigger_conf")
        function_to_unit_test = task.python_callable

        # Mock dependencies
        mock_repo = mock.MagicMock()

        # files to process
        filenames = ["file1/dataset.xml", "file2/dataset.xml"]

        # mocks for trigger_file_processing_elsevier logic
        # It reads file, parses it, and returns confs.
        # We need to mock repo.get_by_id and ElsevierMetadataParser.

        mock_file_bytes = mock.MagicMock()
        mock_repo.get_by_id.return_value = mock_file_bytes

        with (
            mock.patch(
                "elsevier.trigger_file_processing.parse_without_names_spaces"
            ) as mock_parse_xml,
            mock.patch(
                "elsevier.trigger_file_processing.ElsevierMetadataParser"
            ) as MockParser,
        ):
            mock_parse_xml.return_value = "parsed_xml_root"

            mock_parser_instance = MockParser.return_value
            mock_parser_instance.parse.return_value = [
                {"files": {"xml": "full_path_1.xml"}, "meta": "data1"},
                # Assuming one article per dataset for simplicity, or loop handles multiple
            ]

            # Call function
            result = function_to_unit_test(repo=mock_repo, filenames=filenames)

            # Check result: list of confs
            # For 2 files, if each yields 1 article, result len 2
            assert len(result) == 2
            assert result[0] == {
                "file_name": "full_path_1.xml",
                "metadata": {"files": {"xml": "full_path_1.xml"}, "meta": "data1"},
            }

    def test_dag_structure(self):
        assert "elsevier_trigger_file_processing" in self.dag.task_ids
        task = self.dag.get_task("elsevier_trigger_file_processing")
        assert "MappedOperator" in str(type(task)) or task.is_mapped
        assert task.partial_kwargs["trigger_dag_id"] == "elsevier_process_file"
        assert task.partial_kwargs["reset_dag_run"] is True
