from io import BytesIO
from unittest import mock

import pytest
from airflow.models import DagBag
from freezegun import freeze_time


class _S3Object:
    def __init__(self, key):
        self.key = key


def _record_xml(doi):
    return f"""
<record xmlns=\"http://www.openarchives.org/OAI/2.0/\" xmlns:marc=\"http://www.loc.gov/MARC21/slim\">
  <metadata>
    <marc:record>
      <marc:datafield tag=\"024\">
        <marc:subfield code=\"a\">{doi}</marc:subfield>
      </marc:datafield>
    </marc:record>
  </metadata>
</record>
""".strip()


def _snapshot_xml(*records):
    records_xml = "".join(records)
    return f"<wrapper><ListRecords>{records_xml}</ListRecords></wrapper>".encode()


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.fixture(scope="class")
def dag(dagbag):
    return dagbag.dags.get("hindawi_reharvest")


class TestUnitHindawiReharvest:
    def _build_repo(self, payload_by_key):
        mock_repo = mock.MagicMock()
        keys = list(payload_by_key.keys())
        mock_repo.s3_bucket.objects.all.return_value = [_S3Object(key) for key in keys]

        def _get_by_id(key):
            return BytesIO(payload_by_key[key])

        mock_repo.get_by_id.side_effect = _get_by_id
        return mock_repo

    def test_dag_structure(self, dag):
        assert "hindawi_reharvest_trigger_file_processing" in dag.task_ids
        task = dag.get_task("hindawi_reharvest_trigger_file_processing")
        assert "MappedOperator" in str(type(task)) or task.is_mapped
        assert task.partial_kwargs["trigger_dag_id"] == "hindawi_file_processing"
        assert task.partial_kwargs["reset_dag_run"] is True

    def test_collect_records_invalid_combination_file_keys_and_dois(self, dag):
        task = dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        mock_repo = self._build_repo({})

        with pytest.raises(ValueError, match="file_keys and dois"):
            function_to_unit_test(
                repo=mock_repo,
                params={
                    "file_keys": ["2022-09-30/2022-09-30T12:00.xml"],
                    "dois": ["10.1155/2022/1234567"],
                },
            )

    def test_collect_records_date_range_dedup_keeps_newest(self, dag):
        task = dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "2022-09-30/2022-09-30T12:00.xml": _snapshot_xml(
                _record_xml("10.1155/2022/1111111"),
                _record_xml("10.1155/2022/2222222"),
            ),
            "2022-09-30/2022-09-30T13:00.xml": _snapshot_xml(
                _record_xml("10.1155/2022/1111111")
            ),
        }
        mock_repo = self._build_repo(payload_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={
                "date_from": "2022-09-30",
                "date_to": "2022-09-30",
            },
        )

        assert len(result) == 2
        doi_hits = sorted(["10.1155/2022/1111111" in record for record in result])
        assert doi_hits == [False, True]
        latest_key_calls = [call.args[0] for call in mock_repo.get_by_id.call_args_list]
        assert "2022-09-30/2022-09-30T13:00.xml" in latest_key_calls

    def test_collect_records_limit_exceeded_raises(self, dag):
        task = dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "2022-09-30/2022-09-30T12:00.xml": _snapshot_xml(
                _record_xml("10.1155/2022/1111111"),
                _record_xml("10.1155/2022/2222222"),
            )
        }
        mock_repo = self._build_repo(payload_by_key)

        with pytest.raises(ValueError, match="above limit"):
            function_to_unit_test(
                repo=mock_repo,
                params={
                    "date_from": "2022-09-30",
                    "date_to": "2022-09-30",
                    "limit": 1,
                },
            )

    def test_collect_records_file_keys_glob_patterns(self, dag):
        task = dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "2022-09-30/2022-09-30T12:00.xml": _snapshot_xml(
                _record_xml("10.1155/2022/1111111")
            ),
            "2022-09-30/2022-09-30T13:00.xml": _snapshot_xml(
                _record_xml("10.1155/2022/2222222")
            ),
            "2022-10-01/2022-10-01T12:00.xml": _snapshot_xml(
                _record_xml("10.1155/2022/3333333")
            ),
        }
        mock_repo = self._build_repo(payload_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={"file_keys": ["2022-09-30/*"]},
        )

        dois = sorted(
            [
                next(
                    part
                    for part in [record]
                    if "10.1155/2022/1111111" in part or "10.1155/2022/2222222" in part
                )
                for record in result
            ]
        )
        assert len(dois) == 2
        assert any("10.1155/2022/1111111" in record for record in result)
        assert any("10.1155/2022/2222222" in record for record in result)

    @freeze_time("2022-10-02")
    def test_collect_records_dois_without_dates_defaults_last_year(self, dag):
        task = dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "2020-01-01/2020-01-01T10:00.xml": _snapshot_xml(
                _record_xml("10.1155/2020/9999999")
            ),
            "2022-09-30/2022-09-30T12:00.xml": _snapshot_xml(
                _record_xml("10.1155/2022/1234567")
            ),
        }
        mock_repo = self._build_repo(payload_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={
                "dois": ["10.1155/2022/1234567"],
            },
        )

        assert len(result) == 1
        assert "10.1155/2022/1234567" in result[0]

    def test_collect_records_dois_range_too_large_raises(self, dag):
        task = dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        mock_repo = self._build_repo({})

        with pytest.raises(ValueError, match="must not exceed one year"):
            function_to_unit_test(
                repo=mock_repo,
                params={
                    "dois": ["10.1155/2022/1234567"],
                    "date_from": "2020-01-01",
                    "date_to": "2022-01-01",
                },
            )

    def test_prepare_trigger_conf_dry_run_and_normal(self, dag):
        task = dag.get_task("prepare_trigger_conf")
        function_to_unit_test = task.python_callable

        records = [_record_xml("10.1155/2022/1234567")]

        dry_run_result = function_to_unit_test(
            records=records, params={"dry_run": True}
        )
        assert dry_run_result == []

        normal_result = function_to_unit_test(
            records=records, params={"dry_run": False}
        )
        assert len(normal_result) == 1
        assert normal_result[0]["record"] == records[0]
