
import sys
import os
import configparser

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .logging_conf import setup_logging


logger = setup_logging()

conf = configparser.ConfigParser()
conf.read(os.path.join(os.path.dirname(__file__), "../config/config.conf"))

# AWS configuration
AWS_ACCESS_KEY = conf.get("aws", "aws_access_key")
AWS_SECRET_ACCESS_KEY = conf.get("aws", "aws_secret_key")
AWS_REGION = conf.get("aws", "aws_region")
AWS_BUCKET_NAME_INPUT = conf.get("aws", "aws_bucket_name_input")
AWS_BUCKET_NAME_OUPUT = conf.get("aws", "aws_bucket_name_output")

# API configuration
API_KEY = conf.get("api", "api_key")
API_URL = conf.get("api", "api_url")
BASE_URL = conf.get("api", "base_url")
raw_regions = conf.get("api", "api_regions")
API_REGIONS = [r.strip().strip('"').strip("'") for r in raw_regions.split(",")]

# Data configuration
OUTPUT_DIR = conf.get("data", "output_dir")
INPUT_DIR = conf.get("data", "input_dir")