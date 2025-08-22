

import sys
import os
import s3fs
import requests
import json
import glob
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import logger, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_REGION,\
    API_KEY, API_REGIONS, BASE_URL, INPUT_DIR


class BucketOperation:  
    
    def connect_to_s3(self) -> bool:
        s3_connection = True 
        try:
            s3_connection = s3fs.S3FileSystem(
                anon=False,
                key=AWS_ACCESS_KEY,
                secret=AWS_SECRET_ACCESS_KEY,
                client_kwargs={
                    'region_name': AWS_REGION
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
        input_dir = INPUT_DIR
        os.makedirs(input_dir, exist_ok=True)

        for region in regions:
            url = f"{base_url}/{region}?unitGroup=metric&key={api_key}&include=days"
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    file_path = os.path.join(input_dir, f"{region.replace(' ', '_')}.json")
                    with open(file_path, "w") as f:
                        json.dump(response.json(), f, indent=2)
                    logger.info(f"Data Saved in: {region}")
                else:
                    logger.error(f"Failed while fetching data: {region} — Status {response.status_code}")
            except Exception as e:
                data_fetched = False
                logger.error(f"Error: {region} — {e}")
        return data_fetched


    def concatenate_data(self, file_list: list):
        is_concatenated = True
        new_file_name =  f"regions_{datetime.now().strftime('%Y%m%d')}.json"
        output_file = os.path.join(INPUT_DIR, new_file_name)
        combined_data = []
        try:
            for file_path in file_list:
                if glob.fnmatch.fnmatch(os.path.basename(file_path), 'regions_*.json'):
                    continue
                with open(file_path, 'r') as infile:
                    data = json.load(infile)
                    combined_data.append(data)
                os.remove(file_path)
                logger.info(f"File {file_path} is removed successfully.")
            
            with open(output_file, "w") as outfile:
                json.dump(combined_data, outfile, indent=4)
            logger.info(f"File are concatenate successfuly and saved to {output_file}")

        except Exception as e:
            is_concatenated = False
            logger.error(f"Error while concatenating data: {e}")
        return is_concatenated


    def upload_data_into_bucket(self, source_path: str, bucket_name: str, s3: s3fs.S3FileSystem) -> bool:
        data_uploaded = True
        try:
            file_paths = glob.glob(os.path.join(source_path, "*.json"))
            for i in file_paths:
                s3.put(f"{i}", f"{bucket_name}/{os.path.basename(i)}")
            logger.info(f"Data uploaded to bucket {bucket_name} successfully.")
        except Exception as e:
            data_uploaded = False
            logger.error("Error while uploading data to bucket {0}: {1}".format(bucket_name, e))

        return data_uploaded