import logging

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from common.pull_ftp import migrate_from_ftp as migrate_from_ftp_common
from common.pull_ftp import reprocess_files
from elsevier.repository import ElsevierRepository
from elsevier.sftp_service import ElsevierSFTPService
from elsevier.trigger_file_processing import trigger_file_processing_elsevier

logger = logging.getLogger("airflow.task")

ELSEVIER_REPO = ElsevierRepository()
ELSEVIER_SFTP = ElsevierSFTPService()


@dag(
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="5 */6 * * *",
    tags=["pull", "elsevier"],
    params={
        "excluded_directories": [],
        "force_pull": False,
        "filenames_pull": {"enabled": False, "filenames": [], "force_from_ftp": False},
    },
)
def elsevier_pull_sftp():
    @task()
    def migrate_from_ftp(sftp=ELSEVIER_SFTP, repo=ELSEVIER_REPO, **kwargs):
        params = kwargs["params"]
        specific_files = (
            "filenames_pull" in params
            and params["filenames_pull"]["enabled"]
            and not params["filenames_pull"]["force_from_ftp"]
        )
        if specific_files:
            specific_files_names = reprocess_files(repo, logger, **kwargs)
            return specific_files_names

        with sftp:
            return migrate_from_ftp_common(
                sftp, repo, logger, publisher="elsevier", **kwargs
            )

    @task()
    def prepare_trigger_conf(
        repo=ELSEVIER_REPO,
        filenames=None,
    ):
        return trigger_file_processing_elsevier(
            publisher="elsevier", repo=repo, logger=logger, filenames=filenames or []
        )

    archive_names = migrate_from_ftp()
    trigger_confs = prepare_trigger_conf(filenames=archive_names)

    TriggerDagRunOperator.partial(
        task_id="elsevier_trigger_file_processing",
        trigger_dag_id="elsevier_process_file",
        reset_dag_run=True,
    ).expand(conf=trigger_confs)


dag_taskflow = elsevier_pull_sftp()
