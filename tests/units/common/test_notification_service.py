from unittest.mock import MagicMock, patch

import pytest
import requests
from common.notification_service import (
    FailedDagNotifier,
    dag_failure_callback,
    send_zulip_message,
)


@pytest.fixture
def zulip_env(monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://zulip.example.com")
    monkeypatch.setenv("ZULIP_BOT_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_BOT_API_KEY", "secret")
    monkeypatch.setenv("ZULIP_STREAM", "alerts")
    monkeypatch.setenv("ZULIP_TOPIC", "DAG failures")


class TestSendZulipMessage:
    def test_sends_post_to_zulip_api(self, zulip_env):
        with patch("common.notification_service.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            send_zulip_message("test message")

        mock_post.assert_called_once_with(
            "https://zulip.example.com/api/v1/messages",
            auth=("bot@example.com", "secret"),
            data={
                "type": "stream",
                "to": "alerts",
                "topic": "DAG failures",
                "content": "test message",
            },
            timeout=10,
        )

    def test_skips_when_site_empty(self, monkeypatch):
        monkeypatch.setenv("ZULIP_SITE", "")
        monkeypatch.setenv("ZULIP_BOT_EMAIL", "bot@example.com")
        monkeypatch.setenv("ZULIP_BOT_API_KEY", "secret")

        with patch("common.notification_service.requests.post") as mock_post:
            send_zulip_message("test message")

        mock_post.assert_not_called()

    def test_skips_when_credentials_empty(self, monkeypatch):
        monkeypatch.setenv("ZULIP_SITE", "https://zulip.example.com")
        monkeypatch.setenv("ZULIP_BOT_EMAIL", "")
        monkeypatch.setenv("ZULIP_BOT_API_KEY", "")

        with patch("common.notification_service.requests.post") as mock_post:
            send_zulip_message("test message")

        mock_post.assert_not_called()

    def test_raises_on_http_error(self, zulip_env):
        with patch("common.notification_service.requests.post") as mock_post:
            mock_post.return_value = MagicMock(
                status_code=403,
                raise_for_status=MagicMock(side_effect=requests.HTTPError("403")),
            )
            with pytest.raises(requests.HTTPError):
                send_zulip_message("test message")


class TestDagFailureCallback:
    def _make_context(self, dag_id="test_dag", run_id="run_123"):
        dag = MagicMock()
        dag.dag_id = dag_id
        return {"dag": dag, "run_id": run_id}

    def test_sends_notification_with_dag_info(self, zulip_env, monkeypatch):
        monkeypatch.setenv("AIRFLOW_BASE_URL", "http://airflow.example.com")
        context = self._make_context(dag_id="aps_pull_api")

        with patch("common.notification_service.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            dag_failure_callback(context)

        mock_post.assert_called_once()
        message = mock_post.call_args[1]["data"]["content"]
        assert "aps_pull_api" in message
        assert "run_123" in message
        assert (
            "http://airflow.example.com/dags/aps_pull_api/runs/run_123/?state=failed"
            in message
        )

    def test_base_url_without_scheme(self, zulip_env, monkeypatch):
        monkeypatch.setenv("AIRFLOW_BASE_URL", "scoap3-dev.siscern.org")
        context = self._make_context(dag_id="aps_pull_api")

        with patch("common.notification_service.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            dag_failure_callback(context)

        message = mock_post.call_args[1]["data"]["content"]
        assert (
            "https://scoap3-dev.siscern.org/dags/aps_pull_api/runs/run_123/?state=failed"
            in message
        )

    def test_does_not_raise_when_zulip_fails(self, zulip_env):
        context = self._make_context()

        with patch(
            "common.notification_service.requests.post",
            side_effect=Exception("network error"),
        ):
            dag_failure_callback(context)


class TestFailedDagNotifier:
    def _make_context(self, dag_id="test_dag", run_id="run_123"):
        dag = MagicMock()
        dag.dag_id = dag_id
        return {"dag": dag, "run_id": run_id}

    def test_notify_sends_zulip_message(self, zulip_env, monkeypatch):
        monkeypatch.setenv("AIRFLOW_BASE_URL", "http://airflow.example.com")
        context = self._make_context(dag_id="aps_pull_api")

        with patch("common.notification_service.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            FailedDagNotifier().notify(context)

        mock_post.assert_called_once()
        message = mock_post.call_args[1]["data"]["content"]
        assert "aps_pull_api" in message

    def test_notify_does_not_raise_when_zulip_fails(self, zulip_env):
        context = self._make_context()

        with patch(
            "common.notification_service.requests.post",
            side_effect=Exception("network error"),
        ):
            FailedDagNotifier().notify(context)
