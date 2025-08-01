

import sys
import os
import pytest
import s3fs
import json
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tasks.weather_etl import BucketOperation
from utils.constants import AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY,\
      AWS_REGION, BASE_URL, API_KEY, API_REGIONS
     
def test_connect_to_s3(mocker):
    mock_s3 = mocker.patch('tasks.weather_etl.s3fs.S3FileSystem')
    mock_instance = mock_s3.return_value

    bucket = BucketOperation()
    result = bucket.connect_to_s3()

    mock_s3.assert_called_once_with(
        anon=False,
        key=AWS_ACCESS_KEY,
        secret=AWS_SECRET_ACCESS_KEY,
        client_kwargs={
            'region': AWS_REGION
        }
    )
    assert result == mock_instance

def test_create_bucket(mocker):
    mock_s3 = mocker.patch('tasks.weather_etl.s3fs.S3FileSystem')
    mock_instance = mock_s3.return_value
    bucket_name = "test-bucket"

    mock_instance.exists.return_value = False

    bucket = BucketOperation()
    result = bucket.create_bucket(mock_instance, bucket_name)

    mock_instance.mkdir.assert_called_once_with(bucket_name)
    assert result is True

    mock_instance.exists.return_value = True
    result = bucket.create_bucket(mock_instance, bucket_name)

    mock_instance.mkdir.assert_called_once()
    assert result is True


def test_create_bucket_failure(mocker):
    mock_s3 = mocker.patch('tasks.weather_etl.s3fs.S3FileSystem')
    mock_instance = mock_s3.return_value
    bucket_name = "test-bucket"

    mock_instance.exists.side_effect = Exception("S3 error")

    bucket = BucketOperation()
    result = bucket.create_bucket(mock_instance, bucket_name)

    assert result is False
    mock_instance.mkdir.assert_not_called()


def test_fetch_raw_data_region_weather(mocker):
    mock_requests = mocker.patch('tasks.weather_etl.requests.get')
    mock_response = mock_requests.return_value
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "sample data"}

    bucket = BucketOperation()
    bucket.fetch_raw_data_region_weather()
    base_url = BASE_URL
    api_key = API_KEY
    regions = API_REGIONS

    for region in regions:
        url = f"{base_url}/{region}?unitGroup=metric&key={api_key}&include=days"
        mock_requests.assert_any_call(url)
        assert mock_response.status_code == 200
        assert mock_response.json()

def test_upload_data_into_bucket(mocker):
     with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test_file.json")
        with open(file_path, "w") as f:
            json.dump({"region": "test", "temp": 25}, f)

        mock_s3 = mocker.patch('tasks.weather_etl.s3fs.S3FileSystem')
        mock_instance = mock_s3.return_value

        bucket_name = "test-bucket"

        bucket = BucketOperation()
        result = bucket.upload_data_into_bucket(tmpdir, bucket_name, mock_instance)

        assert mock_instance.put.call_count == 1
        args, kwargs = mock_instance.put.call_args
        uploaded_local_path, uploaded_s3_path = args

        assert uploaded_s3_path.startswith(bucket_name)
        assert uploaded_local_path.endswith(".json")
        assert result is True