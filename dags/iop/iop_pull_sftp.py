import logging

import common.pull_ftp as pull_ftp
import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from iop.repository import IOPRepository
from iop.sftp_service import IOPSFTPService

logger = logging.getLogger("airflow.task")

IOP_REPO = IOPRepository()
IOP_SFTP = IOPSFTPService()


@dag(
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="25 */6 * * *",
    tags=["pull", "iop"],
    params={
        "excluded_directories": [],
        "force_pull": False,
        "filenames_pull": {"enabled": False, "filenames": [], "force_from_ftp": False},
    },
)
def iop_pull_sftp():
    @task()
    def migrate_from_ftp(repo=IOP_REPO, sftp=IOP_SFTP, **kwargs):
        params = kwargs["params"]
        specific_files = (
            "filenames_pull" in params
            and params["filenames_pull"]["enabled"]
            and not params["filenames_pull"]["force_from_ftp"]
        )
        if specific_files:
            specific_files_names = pull_ftp.reprocess_files(repo, logger, **kwargs)
            return specific_files_names

        with sftp:
            return pull_ftp.migrate_from_ftp(sftp, repo, logger, **kwargs)

    @task()
    def prepare_trigger_conf(filenames=None):
        return pull_ftp.trigger_file_processing(
            publisher="iop",
            repo=IOP_REPO,
            logger=logger,
            filenames=filenames or [],
        )

    filenames = migrate_from_ftp()
    trigger_confs = prepare_trigger_conf(filenames=filenames)

    TriggerDagRunOperator.partial(
        task_id="iop_trigger_file_processing",
        trigger_dag_id="iop_process_file",
        reset_dag_run=True,
    ).expand(conf=trigger_confs)


dag_taskflow = iop_pull_sftp()
