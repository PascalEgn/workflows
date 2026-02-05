import logging

from airflow.models import DAG
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.standard.operators.python import PythonOperator


def log_dag_info(**context):
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]
    dag = context["dag"]
    msg = (
        f"DAG Metadata | "
        f"dag_id={dag.dag_id} | "
        f"version={ti.dag_version_id} | "
        f"tags={list(dag.tags)} | "
        f"run_id={context.get('run_id')}"
    )
    logger.info(msg)


class GlobalDagMetadataLoggerPlugin(AirflowPlugin):
    name = "global_dag_metadata_logger"

    def on_load(self, *args, **kwargs):
        _original_dag_init = DAG.__init__

        def patched_dag_init(self, *args, **kwargs):
            _original_dag_init(self, *args, **kwargs)
            if getattr(self, "_logger_task_injected", False):
                return
            self._logger_task_injected = True
            if "log_dag_metadata" in self.task_ids:
                return
            log_dag_metadata = PythonOperator(
                task_id="log_dag_metadata", python_callable=log_dag_info, dag=self
            )
            existing_roots = [
                t for t in self.roots if t.task_id != log_dag_metadata.task_id
            ]
            for root in existing_roots:
                log_dag_metadata >> root

        DAG.__init__ = patched_dag_init
