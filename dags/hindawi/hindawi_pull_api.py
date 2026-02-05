import logging
import os

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from common.utils import set_harvesting_interval
from hindawi.hindawi_api_client import HindawiApiClient
from hindawi.hindawi_params import HindawiParams
from hindawi.repository import HindawiRepository
from hindawi.utils import save_file_in_s3, split_xmls

HINDAWI_REPO = HindawiRepository()
logger = logging.getLogger("airflow.task")


@dag(
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="15 */6 * * *",
    tags=["pull", "hindawi"],
    params={"from_date": None, "until_date": None, "record_doi": None},
)
def hindawi_pull_api():
    @task()
    def set_fetching_intervals(repo=HINDAWI_REPO, **kwargs):
        return set_harvesting_interval(repo=repo, **kwargs)

    @task()
    def save_xml_in_s3(dates: dict, repo=HINDAWI_REPO, **kwargs):
        record = kwargs["params"]["record_doi"]
        parameters = HindawiParams(
            from_date=dates["from_date"], until_date=dates["until_date"], record=record
        ).get_params()
        rest_api = HindawiApiClient(
            base_url=os.getenv("HINDAWI_API_BASE_URL", "https://oaipmh.hindawi.com")
        )
        articles_metadata = rest_api.get_articles_metadata(parameters)
        if not articles_metadata:
            logger.warning("No new data is uploaded to s3")
            return
        return save_file_in_s3(data=articles_metadata, repo=repo)

    @task()
    def prepare_trigger_conf(key, repo=HINDAWI_REPO):
        if not key:
            logger.warning("No new files were downloaded to s3")
            return []
        records = split_xmls(repo, key)
        return [{"record": record} for record in records]

    intervals = set_fetching_intervals()
    key = save_xml_in_s3(intervals)
    trigger_confs = prepare_trigger_conf(key)

    TriggerDagRunOperator.partial(
        task_id="hindawi_trigger_file_processing",
        trigger_dag_id="hindawi_file_processing",
        reset_dag_run=True,
    ).expand(conf=trigger_confs)


hindawi_download_files_dag = hindawi_pull_api()
