# Workflows

The following image describes the process for each publishers. Please note that the number of tasks is the minimum, more tasks can be implemented if the need raises.

![DAG Architecture](./documentation/airflow_workflows.png)

## Run with docker compose

The easiest way to run the project is run in with docker compose.
For it docker compose has to be installed. After just run in it the command bellow:

```
docker compose up --build
```

### Script

A Makefile has been created to ease this process. The available targets are the following:

- `make start` : Starts all processes needed.
- `make stop` : Stops all processes needed.
- `make test` : Run tests.

## Access UI

Airflow UI will be running on localhost:8080.
More details about Airflow installation and running can be found [here](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html)

## Environment Variables

### Publisher Specific

| Name                      | Description                                                | Affected Publishers |
| ------------------------- | ---------------------------------------------------------- | ------------------- |
| {PUBLISHER}\_API_BASE_URL | Base URL for the API to call to get the articles metadata. | APS                 |
| {PUBLISHER}\_BUCKET_NAME  | S3 Bucket name                                             | APS                 |
| {PUBLISHER}\_FTP_HOST     | FTP Host                                                   | Springer            |
| {PUBLISHER}\_FTP_USERNAME | FTP Username                                               | Springer            |
| {PUBLISHER}\_FTP_PASSWORD | FTP Password                                               | Springer            |
| {PUBLISHER}\_FTP_PORT     | FTP Port                                                   | Springer            |
| {PUBLISHER}\_FTP_DIR      | FTP Base directory                                         | Springer            |

### Global

| Name        | Description                     |
| ----------- | ------------------------------- |
| REPO_URL    | URL for the SCOAP3 repo schema. |
| S3_ENDPOINT | Endpoint of the S3 server.      |
| S3_USERNAME | Username of the S3 server.      |
| S3_PASSWORD | Password of the S3 server.      |
| S3_REGION   | Username of the S3 server.      |
