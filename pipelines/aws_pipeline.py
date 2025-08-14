

import sys
import os
import glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tasks.weather_etl import BucketOperation
from utils.constants import logger, AWS_BUCKET_NAME_INPUT, OUTPUT_DIR

class AwsPipeline:
    
    def __init__(self):
        self.bucket_operation = BucketOperation()

   
    def run_create_bucket(self):
        s3 = self.bucket_operation.connect_to_s3()
        if not self.bucket_operation.create_bucket(s3, AWS_BUCKET_NAME_INPUT):
            logger.error("Failed to create s3 bucket.")
            return False
        return True
    
    def run_load_data_into_bucket(self):
        s3 = self.bucket_operation.connect_to_s3()
        if not self.bucket_operation.upload_data_into_bucket(OUTPUT_DIR, AWS_BUCKET_NAME_INPUT, s3):
            logger.error("Failed to upload data into bucket.")
            return False
        return True