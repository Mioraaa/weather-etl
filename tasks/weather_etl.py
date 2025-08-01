

import sys
import os
import s3fs
import requests
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import logger, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_REGION,\
    API_KEY, API_REGIONS, BASE_URL, OUTPUT_DIR


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


    def create_bucket(self, s3: s3fs.S3FileSystem, bucket_name: str) -> bool:
        bucket_created = True
        try:
            if not s3.exists(bucket_name):
                s3.mkdir(bucket_name)
                logger.info(f"bucket {bucket_name} created.")
            else:
                logger.warning(f"bucket {bucket_name} already exists.")
        except Exception as e:
            bucket_created = False
            logger.error("Error while creating bucket {0}".format(e))

        return bucket_created


    def fetch_raw_data_region_weather(self):
        data_fetched = True
        api_key = API_KEY
        base_url = BASE_URL
        regions = API_REGIONS
        output_dir = OUTPUT_DIR

        for region in regions:
            url = f"{base_url}/{region}?unitGroup=metric&key={api_key}&include=days"
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    file_path = os.path.join(output_dir, f"{region.replace(' ', '_')}.json")
                    with open(file_path, "w") as f:
                        json.dump(response.json(), f, indent=2)
                    logger.info(f"Data Saved in: {region}")
                    data_fetched = False
                else:
                    logger.error(f"Failed while fetching data: {region} — Status {response.status_code}")
                    data_fetched = False
            except Exception as e:
                data_fetched = False
                logger.error(f"Error: {region} — {e}")
        return data_fetched


    def upload_data_into_bucket(self, file_path: str, bucket_name: str, s3: s3fs.S3FileSystem) -> bool:
        data_uploaded = True
        try:
            s3.put(file_path, f"{bucket_name}/{os.path.basename(file_path)}")
            logger.info(f"Data uploaded to bucket {bucket_name} successfully.")
        except Exception as e:
            data_uploaded = False
            logger.error("Error while uploading data to bucket {0}: {1}".format(bucket_name, e))

        return data_uploaded