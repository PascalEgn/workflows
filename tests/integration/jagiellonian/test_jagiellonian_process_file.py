import os
from datetime import UTC, datetime
from unittest import mock
from urllib.parse import urlparse

import boto3
import pytest
from airflow.models import DagBag
from botocore.client import Config

endpoint = os.getenv("S3_ENDPOINT", "s3")
parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
MINIO_HOST = parsed.hostname or "s3"


@pytest.fixture(scope="class")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.mark.usefixtures("dagbag")
class TestJagiellonianProcessFile:
    def setup_method(self):
        os.environ["AIRFLOW_CONN_AWS_S3_MINIO_TEST"] = (
            f"aws://airflow:Airflow01@{MINIO_HOST}:9000"
            f"?endpoint_url=http%3A%2F%2F{MINIO_HOST}%3A9000"
            "&region_name=us-east-1&verify=false"
        )

        self.dag_id = "jagiellonian_process_file"
        self.execution_date = datetime.now(UTC)

        self.dag = DagBag(dag_folder="dags/", include_examples=False).get_dag(
            self.dag_id
        )
        assert self.dag is not None, f"DAG {self.dag_id} failed to load"

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}:9000",
            aws_access_key_id="airflow",
            aws_secret_access_key="Airflow01",
            region_name="us-east-1",
            config=Config(signature_version="s3v4"),
            verify=False,
        )

        s3.create_bucket(Bucket="jagiellonian-test", ACL="public-read-write")
        response = s3.list_objects_v2(Bucket="jagiellonian-test")

        if "Contents" in response:
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]

            if objects_to_delete:
                s3.delete_objects(
                    Bucket="jagiellonian", Delete={"Objects": objects_to_delete}
                )

    def teardown_method(self):
        if "AIRFLOW_CONN_AWS_S3_MINIO_TEST" in os.environ:
            del os.environ["AIRFLOW_CONN_AWS_S3_MINIO_TEST"]
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}:9000",
            aws_access_key_id="airflow",
            aws_secret_access_key="Airflow01",
            region_name="us-east-1",
            config=Config(signature_version="s3v4"),
            verify=False,
        )
        response = s3.list_objects_v2(Bucket="jagiellonian-test")

        if "Contents" in response:
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]

            if objects_to_delete:
                s3.delete_objects(
                    Bucket="jagiellonian-test", Delete={"Objects": objects_to_delete}
                )

        s3.delete_bucket(Bucket="jagiellonian-test")

    @mock.patch.dict(
        os.environ,
        {
            "JAGIELLONIAN_BUCKET_NAME": "jagiellonian-test",
            "AWS_CONN_ID": "aws_s3_minio_test",
        },
    )
    def test_save_to_s3(self):
        sample_article = {
            "title": "Test Article",
            "authors": ["Author 1", "Author 2"],
            "abstract": "This is a test abstract",
            "dois": [{"value": "10.1234/test.123"}],
            "files": [],
        }

        task = self.dag.get_task("jagiellonian-save-to-s3")
        function_to_unit_test = task.python_callable

        function_to_unit_test(sample_article)

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}:9000",
            aws_access_key_id="airflow",
            aws_secret_access_key="Airflow01",
            region_name="us-east-1",
            config=Config(signature_version="s3v4"),
            verify=False,
        )

        response = s3.list_objects_v2(Bucket="jagiellonian-test")

        assert "10.1234/test.123" in response["Contents"][0]["Key"]
