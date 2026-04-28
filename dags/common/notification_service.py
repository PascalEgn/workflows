import logging
import os

import requests
from airflow.sdk import BaseNotifier

logger = logging.getLogger("airflow.task")


def send_zulip_message(message: str) -> None:
    site = os.getenv("ZULIP_SITE", "")
    bot_email = os.getenv("ZULIP_BOT_EMAIL", "")
    api_key = os.getenv("ZULIP_BOT_API_KEY", "")
    stream = os.getenv("ZULIP_STREAM", "")
    topic = os.getenv("ZULIP_TOPIC", "")
    if not all([site, bot_email, api_key]):
        logger.warning(
            "Zulip notification skipped: ZULIP_SITE, ZULIP_BOT_EMAIL, or ZULIP_BOT_API_KEY not configured"
        )
        return

    url = f"{site.rstrip('/')}/api/v1/messages"
    response = requests.post(
        url,
        auth=(bot_email, api_key),
        data={
            "type": "stream",
            "to": stream,
            "topic": topic,
            "content": message,
        },
        timeout=10,
    )
    response.raise_for_status()


def dag_failure_callback(context: dict) -> None:
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown"
    run_id = context.get("run_id", "unknown")

    base_url = os.getenv("AIRFLOW_BASE_URL", "localhost:8080")
    if not base_url.startswith(("http://", "https://")):
        base_url = f"https://{base_url}"
    run_url = f"{base_url.rstrip('/')}/dags/{dag_id}/runs/{run_id}/?state=failed"

    message = (
        f":love_letter: **DAG failed**: `{dag_id}`\n"
        f"- **Run ID**: `{run_id}`\n"
        f"- **Failed tasks**: [view in Airflow]({run_url})"
    )
    try:
        send_zulip_message(message)
    except Exception:
        logger.exception("Failed to send Zulip notification for DAG %s", dag_id)


class FailedDagNotifier(BaseNotifier):
    def notify(self, context):
        dag_failure_callback(context)
