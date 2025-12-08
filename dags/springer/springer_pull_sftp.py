import logging

import common.pull_ftp as pull_ftp
import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from springer.repository import SpringerRepository
from springer.sftp_service import SpringerSFTPService

logger = logging.getLogger("airflow.task")

SPRINGER_REPO = SpringerRepository()
SPRINGER_SFTP = SpringerSFTPService()


@dag(
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="55 */6 * * *",
    tags=["pull", "springer"],
    params={
        "excluded_directories": [],
        "force_pull": False,
        "filenames_pull": {"enabled": False, "filenames": [], "force_from_ftp": False},
    },
)
def springer_pull_sftp():
    @task()
    def migrate_from_ftp(repo=SPRINGER_REPO, sftp=SPRINGER_SFTP, **kwargs):
        params = kwargs["params"]
        reprocess_specific_files = (
            "filenames_pull" in params
            and params["filenames_pull"]["enabled"]
            and not params["filenames_pull"]["force_from_ftp"]
        )
        if reprocess_specific_files:
            specific_files_names = pull_ftp.reprocess_files(repo, logger, **kwargs)
            return specific_files_names
        with sftp:
            return pull_ftp.migrate_from_ftp(sftp, repo, logger, **kwargs)

    @task()
    def prepare_trigger_conf(repo=SPRINGER_REPO, filenames=None):
        return pull_ftp.trigger_file_processing(
            publisher="springer", repo=repo, logger=logger, filenames=filenames or []
        )

    filenames = migrate_from_ftp()
    trigger_confs = prepare_trigger_conf(filenames=filenames)

    TriggerDagRunOperator.partial(
        task_id="springer_trigger_file_processing",
        trigger_dag_id="springer_process_file",
        reset_dag_run=True,
    ).expand(conf=trigger_confs)


dag_taskflow = springer_pull_sftp()
