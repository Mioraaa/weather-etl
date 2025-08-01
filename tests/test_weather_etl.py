

import sys
import os
import pytest
import s3fs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.weather_etl import BucketOperation
from utils.constants import AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_REGION
    
def test_connect_to_s3_success(mocker):
    mock_s3 = mocker.patch('etls.weather_etl.s3fs.S3FileSystem')
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