

import s3fs
from utlis.constants import logger, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_REGION


class BucketOperation:  
    
    def connect_to_s3(self):
        try:
            s3 = s3fs.S3FileSystem(
                anon=False,
                key=AWS_ACCESS_KEY,
                secret=AWS_SECRET_ACCESS_KEY,
                client_kwargs={
                    'region': AWS_REGION
                }
            )
            return s3
        except Exception as e:
            logger.error("Error while connecting to S3: {0}".format(e))


    def create_bucket(s3: s3fs.S3FileSystem, bucket_name: str):
        try:
            if not s3.exists(bucket_name):
                s3.mkdir(bucket_name)
                logger.info(f"bucket {bucket_name} created.")
            else:
                logger.warning(f"bucket {bucket_name} already exists.")
        except Exception as e:
            logger.error("Error while creating bucket {0}".format(e))
