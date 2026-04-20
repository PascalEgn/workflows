import base64
from datetime import UTC, datetime
from io import BytesIO
from unittest import mock

import pytest
from airflow.models import DagBag


class _S3Object:
    def __init__(self, key, last_modified):
        self.key = key
        self.last_modified = last_modified


def _xml_with_doi(doi):
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<article>
  <front>
    <article-meta>
      <pub-date pub-type=\"open-access\">
        <day>1</day>
        <month>1</month>
        <year>2025</year>
      </pub-date>
      <article-id pub-id-type=\"doi\">{doi}</article-id>
    </article-meta>
  </front>
</article>
""".encode()


def _xml_with_doi_and_open_access_date(doi, year, month, day):
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<article>
    <front>
        <article-meta>
            <pub-date pub-type=\"open-access\">
                <day>{day}</day>
                <month>{month}</month>
                <year>{year}</year>
            </pub-date>
            <article-id pub-id-type=\"doi\">{doi}</article-id>
        </article-meta>
    </front>
</article>
""".encode()


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.mark.usefixtures("dagbag")
class TestUnitIopReharvest:
    def setup_method(self):
        self.dag_id = "iop_reharvest"
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        self.dag = dagbag.dags.get(self.dag_id)
        assert self.dag is not None, f"DAG {self.dag_id} failed to load"

    def _build_repo(self, payload_by_key, modified_by_key):
        mock_repo = mock.MagicMock()
        mock_repo.EXTRACTED_DIR = "extracted/"
        objects = [_S3Object(key, modified_by_key[key]) for key in payload_by_key]
        mock_repo.s3.objects.filter.return_value.all.return_value = objects

        def _get_by_id(key):
            return BytesIO(payload_by_key[key])

        mock_repo.get_by_id.side_effect = _get_by_id
        return mock_repo

    def test_dag_structure(self):
        assert "iop_reharvest_trigger_file_processing" in self.dag.task_ids
        task = self.dag.get_task("iop_reharvest_trigger_file_processing")
        assert "MappedOperator" in str(type(task)) or task.is_mapped
        assert task.partial_kwargs["trigger_dag_id"] == "iop_process_file"
        assert task.partial_kwargs["reset_dag_run"] is True

    def test_collect_file_keys_file_keys_and_dois(self):
        task = self.dag.get_task("collect_file_keys")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "extracted/2025-07-24T04_56_26_content/a/article.xml": _xml_with_doi(
                "10.1088/one"
            ),
            "extracted/6_1674-1137_49_10_103104_10__1088_1674-1137_add912/article.xml": _xml_with_doi(
                "10.1088/one"
            ),
            "extracted/2025-07-24T04_56_26_content/b/other.xml": _xml_with_doi(
                "10.1088/two"
            ),
        }
        modified_by_key = {
            "extracted/2025-07-24T04_56_26_content/a/article.xml": datetime(
                2025, 7, 24, tzinfo=UTC
            ),
            "extracted/6_1674-1137_49_10_103104_10__1088_1674-1137_add912/article.xml": datetime(
                2025, 9, 1, tzinfo=UTC
            ),
            "extracted/2025-07-24T04_56_26_content/b/other.xml": datetime(
                2025, 7, 25, tzinfo=UTC
            ),
        }

        mock_repo = self._build_repo(payload_by_key, modified_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={
                "file_keys": ["*"],
                "dois": ["10.1088/one"],
            },
        )

        assert result == [
            "extracted/6_1674-1137_49_10_103104_10__1088_1674-1137_add912/article.xml"
        ]

    def test_collect_file_keys_date_range_uses_open_access_pub_date_from_xml(self):
        task = self.dag.get_task("collect_file_keys")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "extracted/6_1674-1137_49_10_103104_10__1088_1674-1137_add912/article.xml": _xml_with_doi_and_open_access_date(
                "10.1088/one", 2025, 9, 5
            ),
            "extracted/another_messy_name/file.xml": _xml_with_doi_and_open_access_date(
                "10.1088/two", 2025, 6, 1
            ),
        }
        modified_by_key = {
            "extracted/6_1674-1137_49_10_103104_10__1088_1674-1137_add912/article.xml": datetime(
                2000, 1, 1, tzinfo=UTC
            ),
            "extracted/another_messy_name/file.xml": datetime(2000, 1, 1, tzinfo=UTC),
        }

        mock_repo = self._build_repo(payload_by_key, modified_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={
                "date_from": "2025-09-01",
                "date_to": "2025-09-30",
            },
        )

        assert result == [
            "extracted/6_1674-1137_49_10_103104_10__1088_1674-1137_add912/article.xml"
        ]

    def test_collect_file_keys_date_range_skips_missing_open_access_pub_date(self):
        task = self.dag.get_task("collect_file_keys")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "extracted/with_open_access_date/article.xml": _xml_with_doi_and_open_access_date(
                "10.1088/one", 2025, 9, 5
            ),
            "extracted/no_open_access_date/file.xml": b"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<article>
  <front>
    <article-meta>
      <article-id pub-id-type=\"doi\">10.1088/two</article-id>
    </article-meta>
  </front>
</article>
""",
        }
        modified_by_key = {
            "extracted/with_open_access_date/article.xml": datetime(
                2000, 1, 1, tzinfo=UTC
            ),
            "extracted/no_open_access_date/file.xml": datetime(2000, 1, 1, tzinfo=UTC),
        }

        mock_repo = self._build_repo(payload_by_key, modified_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={
                "date_from": "2025-09-01",
                "date_to": "2025-09-30",
            },
        )

        assert result == ["extracted/with_open_access_date/article.xml"]

    def test_collect_file_keys_limit_exceeded_raises(self):
        task = self.dag.get_task("collect_file_keys")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "extracted/2025-07-24T04_56_26_content/a/article.xml": _xml_with_doi(
                "10.1088/one"
            ),
            "extracted/2025-07-24T04_56_26_content/b/article.xml": _xml_with_doi(
                "10.1088/two"
            ),
        }
        modified_by_key = {
            "extracted/2025-07-24T04_56_26_content/a/article.xml": datetime(
                2025, 7, 24, tzinfo=UTC
            ),
            "extracted/2025-07-24T04_56_26_content/b/article.xml": datetime(
                2025, 7, 24, tzinfo=UTC
            ),
        }
        mock_repo = self._build_repo(payload_by_key, modified_by_key)

        with pytest.raises(ValueError, match="above limit"):
            function_to_unit_test(
                repo=mock_repo,
                params={
                    "file_keys": ["*"],
                    "limit": 1,
                },
            )

    def test_prepare_trigger_conf_dry_run_and_normal(self):
        task = self.dag.get_task("prepare_trigger_conf")
        function_to_unit_test = task.python_callable

        file_key = "extracted/2025-07-24T04_56_26_content/a/article.xml"
        xml_bytes = _xml_with_doi("10.1088/one")

        mock_repo = mock.MagicMock()
        mock_repo.get_by_id.return_value = BytesIO(xml_bytes)

        dry_run_result = function_to_unit_test(
            file_keys=[file_key],
            repo=mock_repo,
            params={"dry_run": True},
        )
        assert dry_run_result == []

        normal_result = function_to_unit_test(
            file_keys=[file_key],
            repo=mock_repo,
            params={"dry_run": False},
        )
        assert len(normal_result) == 1
        assert normal_result[0]["file_name"] == file_key

        decoded_xml = base64.b64decode(normal_result[0]["file"]).decode("utf-8")
        assert "10.1088/one" in decoded_xml
