

import os
import configparser

conf = configparser.ConfigParser()
conf.read(os.path.join(os.path.dirname(__file__), "../config/aws_conf.config"))

AWS_ACCESS_KEY = conf.get("aws", "aws_access_key")
AWS_SECRET_ACCESS_KEY = conf.get("aws", "aws_secret_acess_key")
AWS_REGION = conf.get("aws", "aws_region")
AWS_BUCKET_NAME = conf.get("aws", "aws_bucket_name")