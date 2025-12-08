import logging

import common.pull_ftp as pull_ftp
import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from oup.ftp_service import OUPSFTPService
from oup.repository import OUPRepository

logger = logging.getLogger("airflow.task")

OUP_REPO = OUPRepository()
OUP_SFTP = OUPSFTPService()


@dag(
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="45 */6 * * *",
    tags=["pull", "oup"],
    params={
        "excluded_directories": [],
        "force_pull": False,
        "filenames_pull": {"enabled": False, "filenames": [], "force_from_ftp": False},
    },
)
def oup_pull_ftp():
    @task()
    def migrate_from_ftp(ftp=OUP_SFTP, repo=OUP_REPO, **kwargs):
        params = kwargs["params"]
        specific_files = (
            "filenames_pull" in params
            and params["filenames_pull"]["enabled"]
            and not params["filenames_pull"]["force_from_ftp"]
        )
        if specific_files:
            return pull_ftp.reprocess_files(repo, logger, **kwargs)

        with ftp:
            return pull_ftp.migrate_from_ftp(ftp, repo, logger, **kwargs)

    @task()
    def prepare_trigger_conf(
        repo=OUP_REPO,
        filenames=None,
    ):
        return pull_ftp.trigger_file_processing(
            publisher="oup", repo=repo, logger=logger, filenames=filenames or []
        )

    filenames = migrate_from_ftp()
    trigger_confs = prepare_trigger_conf(filenames=filenames)

    TriggerDagRunOperator.partial(
        task_id="oup_trigger_file_processing",
        trigger_dag_id="oup_process_file",
        reset_dag_run=True,
    ).expand(conf=trigger_confs)


oup_pull_ftp()
