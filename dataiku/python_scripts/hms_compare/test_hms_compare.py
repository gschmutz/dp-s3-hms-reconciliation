
"""
This module provides integration tests for comparing baseline and recovered Hive Metastore (HMS) table data,
using CSV files stored in S3 (or MinIO) as the source of truth. It supports both PostgreSQL and Trino as
the backend for HMS metadata access.
Key Features:
-------------
- Loads configuration and credentials from environment variables or utility functions.
- Connects to S3/MinIO to download baseline and recovered CSV files containing table counts and fingerprints.
- Dynamically discovers table names from the HMS database.
- Waits for required files to appear in S3 before running tests.
- Downloads files from S3 to local disk for comparison.
- Provides pytest fixtures for setup and teardown, including cleanup of S3 and local files.
- Compares row counts and fingerprints for each table between baseline and recovered datasets.
- Supports parametrized tests for all discovered tables.
Functions:
----------
- file_exists(key): Checks if a file exists in the S3 bucket.
- wait_for_files(): Waits for all required files to appear in S3.
- download_files(): Downloads required files from S3 to local disk.
- compare_table(table): (Deprecated) Compares two CSV files for row-wise differences.
- cleanup(): Deletes test files from S3 and local disk after tests.
- get_count(table, type): Retrieves the row count for a table from the specified CSV file.
- get_fingerprint(table, type): Retrieves the fingerprint for a table from the specified CSV file.
Pytest Fixtures and Tests:
-------------------------
- setup_before_all(): Session-scoped fixture for setup and teardown.
- test_compare_table_count(table): Parametrized test to compare row counts for each table.
- test_compare_table_fingerprint(table): Parametrized test to compare fingerprints for each table.
Dependencies:
-------------
- boto3, pandas, sqlalchemy, pytest, and custom utility modules.
Environment Variables / Parameters:
-----------------------------------
- ENV, HMS_DB_ACCESS_STRATEGY, HMS_DB_USER, HMS_DB_PASSWORD, HMS_DB_HOST, HMS_DB_PORT, HMS_DB_DBNAME,
    HMS_TRINO_USER, HMS_TRINO_PASSWORD, HMS_TRINO_HOST, HMS_TRINO_PORT, HMS_TRINO_CATALOG, HMS_TRINO_USE_SSL,
    S3_UPLOAD_ENABLED, S3_ENDPOINT_URL, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, S3_ADMIN_BUCKET, HMS_BASELINE_OBJECT_NAME
Usage:
------
Run with pytest to execute all table comparison tests after ensuring the required CSV files are present in S3.
"""
import boto3
import os
import sys
import logging
import time
import pandas as pd
import pytest
from urllib.parse import urlparse
from sqlalchemy import create_engine, select, text, Column, Integer, String, DateTime, inspect
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from hms_util import get_table_names
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZONE = get_zone_name(upper=False)

# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=False)
RUN_AS_CRONJOB= get_param('RUN_AS_CRONJOB', 'false').lower() in ['true', 'yes']

HMS_DB_ACCESS_STRATEGY = get_param('HMS_DB_ACCESS_STRATEGY', 'postgresql')

HMS_DB_USER = get_credential('HMS_DB_USER', 'hive')
HMS_DB_PASSWORD = get_credential('HMS_DB_PASSWORD', 'abc123!')
HMS_DB_HOST = get_param('HMS_DB_HOST', 'hive-metastore-db')
HMS_DB_PORT = get_param('HMS_DB_PORT', '5432')
HMS_DB_DBNAME = get_param('HMS_DB_NAME', 'metastore_db')

HMS_TRINO_USER = get_credential('HMS_TRINO_USER', 'trino')
HMS_TRINO_PASSWORD = get_credential('HMS_TRINO_PASSWORD', '')
HMS_TRINO_HOST = get_param('HMS_TRINO_HOST', 'localhost')
HMS_TRINO_PORT = get_param('HMS_TRINO_PORT', '28082')
HMS_TRINO_CATALOG = get_param('HMS_TRINO_CATALOG', 'minio')
HMS_TRINO_USE_SSL = get_param('HMS_TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

# Connect to MinIO or AWS S3
# Read endpoint URL from environment variable, default to localhost MinIO
S3_ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', 'admin')
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', 'abc123abc123')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_ADMIN_BUCKET = replace_vars_in_string(S3_ADMIN_BUCKET, { "zone": ZONE.upper(), "env": ENV.upper() } )
S3_ADMIN_BUCKET_PREFIX = get_param('S3_ADMIN_BUCKET_PREFIX', '')
S3_POLLING_INTERVAL = get_param('S3_POLLING_INTERVAL', '60')

HMS_CREATE_BASELINE_FLAG = get_param('HMS_CREATE_BASELINE_FLAG', 'hms_db_backup_flag.csv')
HMS_BASELINE_OBJECT_NAME = get_param('HMS_BASELINE_OBJECT_NAME', 'baseline_hms.csv')
HMS_RECOVERED_OBJECT_NAME = get_param('HMS_RECOVERED_OBJECT_NAME', 'recovered_hms.csv')


HMS_BASELINE_OBJECT_NAME = replace_vars_in_string(HMS_BASELINE_OBJECT_NAME, { "admin-bucket-prefix": S3_ADMIN_BUCKET_PREFIX, "zone": ZONE, "env": ENV } )
HMS_RECOVERED_OBJECT_NAME = replace_vars_in_string(HMS_RECOVERED_OBJECT_NAME, { "admin-bucket-prefix": S3_ADMIN_BUCKET_PREFIX, "zone": ZONE, "env": ENV } )

if HMS_DB_ACCESS_STRATEGY.lower() == 'postgresql':
    # Construct connection URLs
    hms_db_url = f'postgresql://{HMS_DB_USER}:{HMS_DB_PASSWORD}@{HMS_DB_HOST}:{HMS_DB_PORT}/{HMS_DB_DBNAME}'
    catalog_name = ""
    
    src_engine = create_engine(hms_db_url)
else:
    hms_trino_url = f'trino://{HMS_TRINO_USER}:{HMS_TRINO_PASSWORD}@{HMS_TRINO_HOST}:{HMS_TRINO_PORT}/{HMS_TRINO_CATALOG}'
    if HMS_TRINO_USE_SSL:
        hms_trino_url = f'{hms_trino_url}?protocol=https&verify=false'
    
    catalog_name = f"{HMS_TRINO_CATALOG}."    
    src_engine = create_engine(hms_trino_url)

# Create S3 client configuration
s3_config = {"service_name": "s3"}
if S3_ENDPOINT_URL:
    s3_config["endpoint_url"] = S3_ENDPOINT_URL
    s3_config["verify"] = False  # Disable SSL verification for self-signed certificates
if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    s3_config["aws_access_key_id"] = AWS_ACCESS_KEY
    s3_config["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY   

s3 = boto3.client(**s3_config)

# Dynamically get table names from the source DB
tables = get_table_names(engine=src_engine, catalog_name=catalog_name)
    
file_keys = [HMS_BASELINE_OBJECT_NAME, HMS_RECOVERED_OBJECT_NAME]
local_files = [f'/tmp/baseline.csv', f'/tmp/recovered.csv']

BASELINE = 'baseline'
RECOVERED = 'recovered'


def download_and_parse_csv_flag(object_name: str):
    """
    Gets as input the local file path and parse it in order to set the timestamp that filters
    the HMS DB in order to create the baseline file accordingly.
     
    Args:
        object_name (str): The name of the flag object in S3.      
        
        - Read the CSV file
        - Get the first value of the 'backup_time' column
        - Set the global FILTER_TIMESTAMP variable to this value
    """

    s3.download_file(S3_ADMIN_BUCKET, HMS_CREATE_BASELINE_FLAG, f'/tmp/create_hms_db_baseline_flag.csv')   

    try:
        df = pd.read_csv(f'/tmp/create_hms_db_baseline_flag.csv')
         
        backup_name = df['backup_file_name'].iloc[0]
        HMS_BASELINE_OBJECT_NAME = f"{object_name.removesuffix('.csv')}_for_{backup_name.removesuffix('.tar.gz')}.csv" 
        print( f"Setting HMS DB baseline name to: '{HMS_BASELINE_OBJECT_NAME}'")

        return HMS_BASELINE_OBJECT_NAME

    except FileNotFoundError:
        sys.exit(f"Error: File '{filepath}' not found.")
    except pd.errors.EmptyDataError:
        sys.exit("Error: File is empty.")
    except pd.errors.ParserError:
        sys.exit("Error: File could not be parsed as CSV.")

def file_exists(key):
    """
    Checks if a file with the specified key exists in the S3 bucket.
    Args:
        key (str): The key (path/filename) of the file to check in the S3 bucket.
    Returns:
        bool: True if the file exists in the S3 bucket, False if it does not.
    Raises:
        Error: If an error other than a missing file (404) occurs during the check.
    """

    try:
        s3.head_object(Bucket=S3_ADMIN_BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise Error(" Generic Error in checking files")

def wait_for_files():
    """
    Waits for a set of files to appear by periodically checking their existence.
    This function monitors the presence of files specified in the global `file_keys` list.
    It repeatedly checks if each file exists using the `file_exists` function. If a file is found,
    it marks it as found and prints a message. The function continues polling at intervals defined
    by the global `poll_interval` variable until all files are found.
    Returns:
        None
    """

    print("Waiting for both files to appear...")

    found_files = {key: False for key in file_keys}

    while not all(found_files.values()):
        for key in file_keys:
            if not found_files[key]:
                if file_exists(key):
                    print(f"Found: {key}")
                    found_files[key] = True
        if not all(found_files.values()):
            time.sleep(int(S3_POLLING_INTERVAL))

# === DOWNLOAD FILES ===            
def download_files():
    """
    Downloads files from an S3 bucket to local file paths.

    Iterates over pairs of S3 object keys and corresponding local file paths,
    downloading each file from the specified S3 bucket to the local destination.

    Assumes the existence of the following variables in the enclosing scope:
        - file_keys: List of S3 object keys to download.
        - local_files: List of local file paths to save the downloaded files.
        - bucket_name: Name of the S3 bucket.
        - s3: Boto3 S3 client or compatible object with a download_file method.
    """
    for key, local_path in zip(file_keys, local_files):
        print(f"Downloading {key} to {local_path}")
        s3.download_file(S3_ADMIN_BUCKET, key, local_path)            

# === NO LONGER NEEDED, kept for the meantime ===        
def compare_table(table: str):
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
        
def cleanup():
    """
    Cleans up resources by deleting specified files from both S3 and the local filesystem.
    This function iterates over a list of S3 object keys and attempts to delete each object from the specified S3 bucket.
    It also iterates over a list of local file paths and attempts to remove each file from the local filesystem.
    Any errors encountered during deletion are caught and printed.
    Assumes the existence of the following variables in the global scope:
        - file_keys: List of S3 object keys to delete.
        - bucket_name: Name of the S3 bucket.
        - s3: Boto3 S3 client instance.
        - local_files: List of local file paths to delete.
        - os: The os module for file operations.
    """

    print("\n Cleaning up...")
    file_keys = [HMS_RECOVERED_OBJECT_NAME]
    # Delete from S3 - To reconsider if needed
    for key in file_keys:
        try:
            s3.delete_object(Bucket=S3_ADMIN_BUCKET, Key=key)
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

def get_count(table: str, type: str) -> int:    
    """
    Returns the count of rows for a specified table from a CSV file, based on the provided type.
    Args:
        table (str): The name of the table to look up.
        type (str): The type indicating which CSV file to read (e.g., BASELINE or another type).
    Returns:
        int: The count of rows for the specified table. Returns 0 if the table is not found.
    """
    if type == BASELINE:
        df = pd.read_csv(local_files[0])
    else:
        df = pd.read_csv(local_files[1])
    
    count = df[df['table_name'] == table]['count'].values
    if len(count) == 0:
        return 0
    return int(count[0])

def get_fingerprint(table: str, type: str) -> str:
    """
    Retrieves the fingerprint value for a specified table from a CSV file, based on the provided type.
    Args:
        table (str): The name of the table to look up.
        type (str): The type indicating which CSV file to use (e.g., BASELINE or another type).
    Returns:
        str: The fingerprint value for the specified table if found; otherwise, an empty string.
    """

    if type == BASELINE:
        df = pd.read_csv(local_files[0])
    else:
        df = pd.read_csv(local_files[1])
    
    fingerprint = df[df['table_name'] == table]['fingerprint'].values
    if len(fingerprint) == 0 or pd.isna(fingerprint[0]):
        return ""
    return fingerprint[0]

if RUN_AS_CRONJOB:
   HMS_BASELINE_OBJECT_NAME = download_and_parse_csv_flag(HMS_BASELINE_OBJECT_NAME)

@pytest.fixture(scope="session", autouse=True)
def setup_before_all():
    print("Setup before all tests")
    # Any global setup can be done here
    wait_for_files()
    download_files()
    yield
    print("Teardown after all tests")
    # Any global teardown can be done here
    cleanup()

@pytest.mark.parametrize("table", tables)
def test_compare_table_count(table: str):
    print(f"Comparing table counts: {table}")
    count_baseline = get_count(table, BASELINE)
    count_recovered = get_count(table, RECOVERED) 
    assert count_baseline == count_recovered, f"Count mismatch for table {table}: baseline={count_baseline}, recovered={count_recovered}"

@pytest.mark.parametrize("table", tables)
def test_compare_table_fingerprint(table: str):
    print(f"Comparing table fingerprints: {table}")
    fingerprint_baseline = get_fingerprint(table, BASELINE)
    fingerprint_recovered = get_fingerprint(table, RECOVERED)

    if fingerprint_baseline != "" and fingerprint_recovered != "":
        assert fingerprint_baseline == fingerprint_recovered, f"Fingerprint mismatch for table {table}: baseline={fingerprint_baseline}, recovered={fingerprint_recovered}"

