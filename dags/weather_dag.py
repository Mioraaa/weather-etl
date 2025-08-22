

import os, sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime
from pipelines.weather_pipeline import WeatherPipeline

default_args = {
    "owner": Variable.get("owner"),
    "start_date": datetime(2025, 1, 7),
}


@task(task_id="connect_to_s3")
def connect_to_s3():
    weather_pipeline = WeatherPipeline()
    return weather_pipeline.run_connect_s3()

@task(task_id="create_bucket")
def create_bucket():
    weather_pipeline = WeatherPipeline()
    return weather_pipeline.run_create_bucket()

@task(task_id="fetch_raw_data_region_weather")
def fetch_raw_data_region_weather():
    weather_pipeline = WeatherPipeline()
    return weather_pipeline.run_fetch_raw_data_region_weather()

@task(task_id="concatenate_data")
def concatenate_data():
    weather_pipeline = WeatherPipeline()
    return weather_pipeline.run_concatenate_data()

@task(task_id="upload_data_into_bucket")
def upload_data_into_bucket():
    weather_pipeline = WeatherPipeline()
    return weather_pipeline.run_upload_data_into_bucket()



with DAG(
    dag_id=Variable.get("dag_id"),
    default_args=default_args,
    schedule=Variable.get("schedule"),
    catchup=False,
) as dag:
    connect_to_s3_task = connect_to_s3()
    create_bucket_task = create_bucket()
    fetch_raw_data_region_weather_task = fetch_raw_data_region_weather()
    concatenate_data_task = concatenate_data()
    upload_data_into_bucket_task = upload_data_into_bucket()

# Run order tasks
connect_to_s3_task >> create_bucket_task >> fetch_raw_data_region_weather_task
fetch_raw_data_region_weather_task >> concatenate_data_task >> upload_data_into_bucket_task
