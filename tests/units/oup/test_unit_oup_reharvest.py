import base64
from io import BytesIO
from unittest import mock

import pytest
from airflow.models import DagBag


class _S3Object:
    def __init__(self, key):
        self.key = key


def _xml_with_doi(doi):
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<article>
  <front>
    <article-meta>
      <article-id pub-id-type=\"doi\">{doi}</article-id>
    </article-meta>
  </front>
</article>
""".encode()


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.mark.usefixtures("dagbag")
class TestUnitOupReharvest:
    def setup_method(self):
        self.dag_id = "oup_reharvest"
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        self.dag = dagbag.dags.get(self.dag_id)
        assert self.dag is not None, f"DAG {self.dag_id} failed to load"

    def _build_repo(self, payload_by_key):
        mock_repo = mock.MagicMock()
        mock_repo.EXTRACTED_DIR = "extracted/"
        keys = list(payload_by_key.keys())
        mock_repo.s3.objects.filter.return_value.all.return_value = [
            _S3Object(key) for key in keys
        ]

        def _get_by_id(key):
            return BytesIO(payload_by_key[key])

        mock_repo.get_by_id.side_effect = _get_by_id
        return mock_repo

    def test_dag_structure(self):
        assert "oup_reharvest_trigger_file_processing" in self.dag.task_ids
        task = self.dag.get_task("oup_reharvest_trigger_file_processing")
        assert "MappedOperator" in str(type(task)) or task.is_mapped
        assert task.partial_kwargs["trigger_dag_id"] == "oup_process_file"
        assert task.partial_kwargs["reset_dag_run"] is True

    def test_collect_records_file_keys_and_dois_filters_resolved_keys(self):
        task = self.dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "extracted/August/15-08-2025/ptep_iss_2025_8_part1.xml/ptac108.xml": _xml_with_doi(
                "10.1093/ptep/ptac108"
            ),
            "extracted/December/19-12-2025/ptep_iss_2025_12_part5.xml/ptac108.xml": _xml_with_doi(
                "10.1093/ptep/ptac108"
            ),
            "extracted/January/30-01-2026/ptep_iss_2025_12_part7.xml/ptac120.xml": _xml_with_doi(
                "10.1093/ptep/ptac120"
            ),
        }
        mock_repo = self._build_repo(payload_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={
                "file_keys": ["*"],
                "dois": ["10.1093/ptep/ptac108"],
            },
        )

        assert result == [
            "extracted/December/19-12-2025/ptep_iss_2025_12_part5.xml/ptac108.xml"
        ]

    def test_collect_records_date_range_dedup_keeps_newest(self):
        task = self.dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "extracted/August/15-08-2025/ptep_iss_2025_8_part1.xml/ptac108.xml": _xml_with_doi(
                "10.1093/ptep/ptac108"
            ),
            "extracted/January/30-01-2026/ptep_iss_2025_12_part7.xml/ptac108.xml": _xml_with_doi(
                "10.1093/ptep/ptac108"
            ),
            "extracted/December/19-12-2025/ptep_iss_2025_12_part5.xml/ptac113.xml": _xml_with_doi(
                "10.1093/ptep/ptac113"
            ),
            "extracted/not-scoap/2026-01-30_00:00:00_ptep_iss_2025_12_part7.xml/ptac999.xml": _xml_with_doi(
                "10.1093/ptep/ptac999"
            ),
        }
        mock_repo = self._build_repo(payload_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={
                "date_from": "2025-08-15",
                "date_to": "2026-01-30",
            },
        )

        assert len(result) == 2
        assert (
            "extracted/January/30-01-2026/ptep_iss_2025_12_part7.xml/ptac108.xml"
            in result
        )
        assert (
            "extracted/December/19-12-2025/ptep_iss_2025_12_part5.xml/ptac113.xml"
            in result
        )

    def test_collect_records_file_keys_limit_exceeded_raises(self):
        task = self.dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "extracted/December/19-12-2025/ptep_iss_2025_12_part5.xml/ptac108.xml": _xml_with_doi(
                "10.1093/ptep/ptac108"
            ),
            "extracted/December/19-12-2025/ptep_iss_2025_12_part5.xml/ptac113.xml": _xml_with_doi(
                "10.1093/ptep/ptac113"
            ),
        }
        mock_repo = self._build_repo(payload_by_key)

        with pytest.raises(ValueError, match="above limit"):
            function_to_unit_test(
                repo=mock_repo,
                params={
                    "file_keys": [
                        "extracted/December/19-12-2025/ptep_iss_2025_12_part5.xml/ptac108.xml",
                        "extracted/December/19-12-2025/ptep_iss_2025_12_part5.xml/ptac113.xml",
                    ],
                    "limit": 1,
                },
            )

    def test_collect_records_glob_patterns(self):
        task = self.dag.get_task("collect_records")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "extracted/August/15-08-2025/ptep_iss_2025_8_part1.xml/ptac108.xml": _xml_with_doi(
                "10.1093/ptep/ptac108"
            ),
            "extracted/December/19-12-2025/ptep_iss_2025_12_part5.xml/ptac113.xml": _xml_with_doi(
                "10.1093/ptep/ptac113"
            ),
            "extracted/January/30-01-2026/ptep_iss_2025_12_part7.xml/ptac120.xml": _xml_with_doi(
                "10.1093/ptep/ptac120"
            ),
        }
        mock_repo = self._build_repo(payload_by_key)

        result_all = function_to_unit_test(
            repo=mock_repo,
            params={"file_keys": ["*"]},
        )
        assert set(result_all) == set(payload_by_key.keys())

        result_august = function_to_unit_test(
            repo=mock_repo,
            params={"file_keys": ["August/*"]},
        )
        assert result_august == [
            "extracted/August/15-08-2025/ptep_iss_2025_8_part1.xml/ptac108.xml"
        ]

        result_mixed = function_to_unit_test(
            repo=mock_repo,
            params={
                "file_keys": [
                    "extracted/August/15-08-2025/ptep_iss_2025_8_part1.xml/ptac108.xml",
                    "extracted/August/*",
                ]
            },
        )
        assert result_mixed == [
            "extracted/August/15-08-2025/ptep_iss_2025_8_part1.xml/ptac108.xml"
        ]

    def test_prepare_trigger_conf_dry_run_and_normal(self):
        task = self.dag.get_task("prepare_trigger_conf")
        function_to_unit_test = task.python_callable

        file_key = "extracted/August/15-08-2025/ptep_iss_2025_8_part1.xml/ptac108.xml"
        xml_bytes = _xml_with_doi("10.1093/ptep/ptac108")
        mock_repo = self._build_repo({file_key: xml_bytes})

        file_keys = [file_key]

        dry_run_result = function_to_unit_test(
            file_keys=file_keys,
            repo=mock_repo,
            params={"dry_run": True},
        )
        assert dry_run_result == []

        normal_result = function_to_unit_test(
            file_keys=file_keys,
            repo=mock_repo,
            params={"dry_run": False},
        )
        assert len(normal_result) == 1
        assert normal_result[0]["file_name"] == file_keys[0]

        decoded_xml = base64.b64decode(normal_result[0]["file"]).decode("utf-8")
        assert "10.1093/ptep/ptac108" in decoded_xml
