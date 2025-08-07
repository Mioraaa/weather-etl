

import sys
import os
import glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tasks.weather_etl import BucketOperation
from utils.constants import logger, AWS_BUCKET_NAME, INPUT_DIR

class WeatherPipeline:
    
    def __init__(self):
        self.bucket_operation = BucketOperation()

    def run_connect_s3(self):
        s3 = self.bucket_operation.connect_to_s3()
        if not s3:
            logger.error("Failed to connect to S3.")
            return False
        return True 

    def run_create_bucket(self):
        s3 = self.bucket_operation.connect_to_s3()
        if not self.bucket_operation.create_bucket(s3, AWS_BUCKET_NAME):
            logger.error("Failed to create s3 bucket.")
            return False
        return True

    def run_fetch_raw_data_region_weather(self):
        if not self.bucket_operation.fetch_raw_data_region_weather():
            logger.error("Failed to fetch raw data for region weather.")
            return False
        return True
    
    def run_concatenate_data(self):
        file_list = glob.glob(os.path.join(INPUT_DIR, "*.json"))
        if not self.bucket_operation.concatenate_data(file_list):
            logger.error("Failed to concatenate data.")
            return False
        return True

    def run_upload_data_into_bucket(self):
        s3 = self.bucket_operation.connect_to_s3()
        if not self.bucket_operation.upload_data_into_bucket(INPUT_DIR, AWS_BUCKET_NAME, s3):
            logger.error("Failed to upload data into bucket.")
            return False
        return True