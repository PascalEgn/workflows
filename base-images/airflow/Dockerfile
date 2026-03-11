FROM registry.cern.ch/docker.io/apache/airflow:slim-3.1.8-python3.12 AS base
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libleveldb-dev \
    && rm -rf /var/lib/apt/lists/*
USER airflow
WORKDIR /opt/airflow

ENV PYTHONBUFFERED=0


COPY --chown=airflow:0 base-images/airflow/providers/apache_airflow_providers_elasticsearch-6.5.0-py3-none-any.whl ./apache_airflow_providers_elasticsearch-6.5.0-py3-none-any.whl

COPY --chown=airflow:0 base-images/airflow/requirements/requirements.txt ./requirements.txt
COPY --chown=airflow:0 base-images/airflow/requirements/requirements-test.txt ./requirements-test.txt
COPY --chown=airflow:0 base-images/airflow/requirements/requirements-airflow.txt ./requirements-airflow.txt

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools wheel
RUN pip install --no-cache-dir -r requirements-airflow.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements-test.txt
RUN pip install --no-cache-dir ./apache_airflow_providers_elasticsearch-6.5.0-py3-none-any.whl
RUN rm requirements.txt requirements-test.txt requirements-airflow.txt ./apache_airflow_providers_elasticsearch-6.5.0-py3-none-any.whl

FROM base AS dev
COPY base-images/airflow/plugins/ /opt/airflow/plugins/

FROM dev AS prod
COPY dags/ /opt/airflow/dags/
