import json
from io import BytesIO
from unittest import mock

import pytest
from airflow.models import DagBag
from freezegun import freeze_time


class _S3Object:
    def __init__(self, key):
        self.key = key


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.fixture(scope="class")
def dag(dagbag):
    return dagbag.dags.get("aps_reharvest")


class TestUnitApsReharvest:
    def _build_repo(self, payload_by_key):
        mock_repo = mock.MagicMock()
        keys = list(payload_by_key.keys())
        mock_repo.s3_bucket.objects.all.return_value = [_S3Object(key) for key in keys]

        def _get_by_id(key):
            return BytesIO(json.dumps(payload_by_key[key]).encode("utf-8"))

        mock_repo.get_by_id.side_effect = _get_by_id
        return mock_repo

    def test_dag_structure(self, dag):
        assert "aps_reharvest_trigger_file_processing" in dag.task_ids
        task = dag.get_task("aps_reharvest_trigger_file_processing")
        assert "MappedOperator" in str(type(task)) or task.is_mapped
        assert task.partial_kwargs["trigger_dag_id"] == "aps_process_file"
        assert task.partial_kwargs["reset_dag_run"] is True

    def test_collect_articles_invalid_combination_file_keys_and_dois(self, dag):
        task = dag.get_task("collect_articles")
        function_to_unit_test = task.python_callable

        mock_repo = self._build_repo({})

        with pytest.raises(ValueError, match="file_keys and dois"):
            function_to_unit_test(
                repo=mock_repo,
                params={
                    "file_keys": ["2026-03-11/2026-03-11T12:00.json"],
                    "dois": ["10.1103/example"],
                },
            )

    def test_collect_articles_date_range_dedup_keeps_newest(self, dag):
        task = dag.get_task("collect_articles")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "2026-03-11/2026-03-11T12:00.json": {
                "data": [
                    {
                        "id": "10.1103/a",
                        "identifiers": {"doi": "10.1103/a"},
                        "title": {"value": "Old version"},
                        "last_modified_at": "2026-03-11T12:31:08+0000",
                    },
                    {
                        "id": "10.1103/b",
                        "identifiers": {"doi": "10.1103/b"},
                        "title": {"value": "Single version"},
                        "last_modified_at": "2026-03-11T12:31:08+0000",
                    },
                ]
            },
            "2026-03-11/2026-03-11T13:00.json": {
                "data": [
                    {
                        "id": "10.1103/a",
                        "identifiers": {"doi": "10.1103/a"},
                        "title": {"value": "New version"},
                        "last_modified_at": "2026-03-11T13:31:08+0000",
                    }
                ]
            },
        }
        mock_repo = self._build_repo(payload_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={
                "date_from": "2026-03-11",
                "date_to": "2026-03-11",
            },
        )

        assert len(result) == 2
        doi_to_title = {
            (record.get("identifiers") or {}).get("doi"): record.get("title", {}).get(
                "value"
            )
            for record in result
        }
        assert doi_to_title["10.1103/a"] == "New version"
        assert doi_to_title["10.1103/b"] == "Single version"

    def test_collect_articles_limit_exceeded_raises(self, dag):
        task = dag.get_task("collect_articles")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "2026-03-11/2026-03-11T12:00.json": {
                "data": [
                    {"id": "10.1103/a", "identifiers": {"doi": "10.1103/a"}},
                    {"id": "10.1103/b", "identifiers": {"doi": "10.1103/b"}},
                ]
            }
        }
        mock_repo = self._build_repo(payload_by_key)

        with pytest.raises(ValueError, match="above limit"):
            function_to_unit_test(
                repo=mock_repo,
                params={
                    "date_from": "2026-03-11",
                    "date_to": "2026-03-11",
                    "limit": 1,
                },
            )

    def test_collect_articles_file_keys_glob_patterns(self, dag):
        task = dag.get_task("collect_articles")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "2026-03-11/2026-03-11T12:00.json": {
                "data": [
                    {
                        "id": "10.1103/a",
                        "identifiers": {"doi": "10.1103/a"},
                        "last_modified_at": "2026-03-11T12:31:08+0000",
                    }
                ]
            },
            "2026-03-11/2026-03-11T13:00.json": {
                "data": [
                    {
                        "id": "10.1103/b",
                        "identifiers": {"doi": "10.1103/b"},
                        "last_modified_at": "2026-03-11T13:31:08+0000",
                    }
                ]
            },
            "2026-03-12/2026-03-12T12:00.json": {
                "data": [
                    {
                        "id": "10.1103/c",
                        "identifiers": {"doi": "10.1103/c"},
                        "last_modified_at": "2026-03-12T12:31:08+0000",
                    }
                ]
            },
        }
        mock_repo = self._build_repo(payload_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={"file_keys": ["2026-03-11/*"]},
        )

        dois = sorted(
            [(record.get("identifiers") or {}).get("doi") for record in result]
        )
        assert dois == ["10.1103/a", "10.1103/b"]

    @freeze_time("2026-03-12")
    def test_collect_articles_dois_without_dates_defaults_last_year(self, dag):
        task = dag.get_task("collect_articles")
        function_to_unit_test = task.python_callable

        payload_by_key = {
            "2024-01-01/2024-01-01T10:00.json": {
                "data": [
                    {
                        "id": "10.1103/old",
                        "identifiers": {"doi": "10.1103/old"},
                    }
                ]
            },
            "2026-03-11/2026-03-11T12:00.json": {
                "data": [
                    {
                        "id": "10.1103/found",
                        "identifiers": {"doi": "10.1103/found"},
                    }
                ]
            },
        }
        mock_repo = self._build_repo(payload_by_key)

        result = function_to_unit_test(
            repo=mock_repo,
            params={
                "dois": ["10.1103/found"],
            },
        )

        assert len(result) == 1
        assert result[0]["identifiers"]["doi"] == "10.1103/found"

    def test_prepare_trigger_conf_dry_run_and_normal(self, dag):
        task = dag.get_task("prepare_trigger_conf")
        function_to_unit_test = task.python_callable

        articles = [{"id": "10.1103/test", "identifiers": {"doi": "10.1103/test"}}]

        dry_run_result = function_to_unit_test(
            articles=articles, params={"dry_run": True}
        )
        assert dry_run_result == []

        normal_result = function_to_unit_test(
            articles=articles, params={"dry_run": False}
        )
        assert len(normal_result) == 1
        assert "article" in normal_result[0]
        assert json.loads(normal_result[0]["article"])["id"] == "10.1103/test"
