import boto3
import os
import sys
import logging
import time
import pandas as pd
from urllib.parse import urlparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZONE = get_zone_name(upper=True)

# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=True)

# Connect to MinIO or AWS S3
# Read endpoint URL from environment variable, default to localhost MinIO
S3_UPLOAD_ENABLED = get_param('S3_UPLOAD_ENABLED', 'true').lower() in ['true', '1', 'yes']  
S3_ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', 'admin')
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', 'admin123')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
HMS_BASELINE_OBJECT_NAME = get_param('HMS_BASELINE_OBJECT_NAME', 'baseline_hms.csv')

# Create S3 client configuration
s3_config = {"service_name": "s3"}
if S3_ENDPOINT_URL:
    s3_config["endpoint_url"] = S3_ENDPOINT_URL
    s3_config["verify"] = False  # Disable SSL verification for self-signed certificates
if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    s3_config["aws_access_key_id"] = AWS_ACCESS_KEY
    s3_config["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY   

s3 = boto3.client(**s3_config)


# === local varibles ===
bucket_name = S3_ADMIN_BUCKET
poll_interval = 60  # seconds
# === file names ===
file_keys = [HMS_BASELINE_OBJECT_NAME, 'recovered_db_data.csv']
local_files = ['/tmp/baseline.csv', '/tmp/recovered_db.csv']

# === CHECK IF FILE EXISTS ===
def file_exists(key):
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise  Error(" Generic Error in checking files")

# === WAIT FOR BOTH FILES ===
def wait_for_files():
    print("Waiting for both files to appear...")

    found_files = {key: False for key in file_keys}

    while not all(found_files.values()):
        for key in file_keys:
            if not found_files[key]:
                if file_exists(key):
                    print(f"Found: {key}")
                    found_files[key] = True
        if not all(found_files.values()):
            time.sleep(poll_interval)

# === DOWNLOAD FILES ===            
def download_files():
    for key, local_path in zip(file_keys, local_files):
        print(f"Downloading {key} to {local_path}")
        s3.download_file(bucket_name, key, local_path)            

# === COMPARE FILES USING PANDAS ===        
def compare_files():
    try:
        df1 = pd.read_csv(local_files[0])
        df2 = pd.read_csv(local_files[1])

        #sort to make comparison order-independent
        df1_sorted = df1.sort_values(by=df1.columns.tolist()).reset_index(drop=True)
        df2_sorted = df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True)

        # Compare: rows that are different (in either file)
        diff = pd.concat([df1_sorted, df2_sorted]).drop_duplicates(keep=False)

        print("\n=== Differences Between CSV Files ===")
        if diff.empty:
            print("No differences found. Files are identical (row-wise).")
        else:
            print(diff)
    except Exception as e:
        print(f"Error comparing files: {e}")
        
# === DELETE FILES FROM S3 AND LOCAL ===
def cleanup():
    print("\n Cleaning up...")

    # Delete from S3
    for key in file_keys:
        try:
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f" Deleted from S3: {key}")
        except Exception as e:
            print(f" Failed to delete {key} from S3: {e}")

    # Delete local files
    for path in local_files:
        try:
            os.remove(path)
            print(f" Deleted local file: {path}")
        except Exception as e:
            print(f" Failed to delete local file {path}: {e}")
            
            
print(f"--------------------------------------OUTPUT--------------------------------------")
print(file_keys)
wait_for_files()
download_files()
compare_files()
print("\nDone. Exiting.")
cleanup()

print(f"--------------------------------------OUTPUT--------------------------------------")            
