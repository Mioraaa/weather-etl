

import sys
import os
import s3fs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import logger, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_REGION


class BucketOperation:  
    
    def connect_to_s3(self) -> bool:
        s3_connection = True 
        try:
            s3_connection = s3fs.S3FileSystem(
                anon=False,
                key=AWS_ACCESS_KEY,
                secret=AWS_SECRET_ACCESS_KEY,
                client_kwargs={
                    'region': AWS_REGION
                }
            )
            
        except Exception as e:
            s3_connection = False
            logger.error("Error while connecting to S3: {0}".format(e))

        return s3_connection

    def create_bucket(s3: s3fs.S3FileSystem, bucket_name: str):
        try:
            if not s3.exists(bucket_name):
                s3.mkdir(bucket_name)
                logger.info(f"bucket {bucket_name} created.")
            else:
                logger.warning(f"bucket {bucket_name} already exists.")
        except Exception as e:
            logger.error("Error while creating bucket {0}".format(e))
