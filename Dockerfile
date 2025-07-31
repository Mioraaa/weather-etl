FROM apache/airflow:2.0.2

COPY requirements.txt /opt/airflow/requirements.txt

USER root

RUN apt-get update && apt-get install -y gcc python3-dev

USER airflow

RUN pip insttall --no-cache-dir -r /opt/airflow/requirements.txt