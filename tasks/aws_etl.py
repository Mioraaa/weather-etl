

import sys
import os
import glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import logger, AWS_BUCKET_NAME_OUPUT, OUTPUT_DIR

class AnalysisData:
    
    def transform_data(self):
        pass