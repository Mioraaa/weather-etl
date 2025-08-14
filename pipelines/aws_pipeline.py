

import sys
import os
import glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tasks.weather_etl import BucketOperation
from tasks.aws_etl import AnalysisData
from utils.constants import logger, AWS_BUCKET_NAME_OUPUT, OUTPUT_DIR

class AwsPipeline:
    
    def __init__(self):
        self.bucket_operation = BucketOperation()
        self.analysis_data = AnalysisData()

   
    def run_create_bucket(self):
        s3 = self.bucket_operation.connect_to_s3()
        if not self.bucket_operation.create_bucket(s3, AWS_BUCKET_NAME_OUPUT):
            logger.error("Failed to create s3 bucket.")
            return False
        return True
    
   