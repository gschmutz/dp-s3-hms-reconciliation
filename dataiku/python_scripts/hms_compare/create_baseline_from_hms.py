"""
This script generates a baseline fingerprint for tables in the Hive Metastore (HMS) database and uploads the results to an S3-compatible storage (such as MinIO or AWS S3). The baseline includes row counts, fingerprints (MD5 hashes of table rows), and optional creation timestamps for each table. The script supports both PostgreSQL and Trino as backend databases for HMS, and allows filtering of tables via environment variables.
Main functionalities:
- Connects to the HMS database using SQLAlchemy, supporting both PostgreSQL and Trino.
- Retrieves table names, optionally filtered by environment variables.
- For each table, computes:
    - Row count.
    - Fingerprint (MD5 hash) of all rows, ordered by primary key.
    - Maximum creation timestamp, if available.
- Writes the baseline data to a CSV file.
- Uploads the CSV file to an S3-compatible bucket if enabled.
Environment variables and credentials are used for configuration, including database access, S3 endpoint, and filtering options.
Dependencies:
- boto3
- sqlalchemy
- psycopg2 or trino dialect for SQLAlchemy
- Custom utility functions: get_param, get_credential, get_zone_name, replace_vars_in_string
Usage:
- Configure environment variables and credentials as needed.
- Run the script to generate and upload the baseline CSV.
"""
import boto3
import os
import sys
import hashlib
import logging
import time
import pandas as pd
from datetime import datetime
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse
from sqlalchemy import create_engine, select, text, Column, Integer, String, DateTime, inspect
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from hms_util import get_table_names
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string
from dataiku.scenario import Scenario

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZONE = get_zone_name(upper=False)

# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=False)
FILTER_TIMESTAMP = get_param('FILTER_TIMESTAMP', None)  # timestamp in seconds since epoch, e.g. 1693440000 for 2023-08-31 00:00:00 UTC
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
S3_UPLOAD_ENABLED = get_param('S3_UPLOAD_ENABLED', 'true').lower() in ['true', '1', 'yes']  
S3_ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', 'admin')
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', 'abc123abc123')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_POLLING_INTERVAL = get_param('S3_POLLING_INTERVAL', '10')

HMS_CREATE_BASELINE_FLAG = get_param('HMS_CREATE_BASELINE_FLAG', 'hms_db_backup_flag.csv')
HMS_CREATE_BASELINE_FLAG = replace_vars_in_string(HMS_CREATE_BASELINE_FLAG, { "zone": ZONE, "env": ENV } )
HMS_BASELINE_OBJECT_NAME = get_param('HMS_BASELINE_OBJECT_NAME', 'baseline_hms.csv')
HMS_RECOVERED_OBJECT_NAME = get_param('HMS_RECOVERED_OBJECT_NAME', '')

HMS_BASELINE_OBJECT_NAME = HMS_RECOVERED_OBJECT_NAME if HMS_RECOVERED_OBJECT_NAME != '' else HMS_BASELINE_OBJECT_NAME

HMS_BASELINE_OBJECT_NAME = replace_vars_in_string(HMS_BASELINE_OBJECT_NAME, { "zone": ZONE, "env": ENV } )
HMS_BASELINE_FILE_NAME = HMS_BASELINE_OBJECT_NAME.replace('/', '__')

file_keys = [HMS_CREATE_BASELINE_FLAG]
local_files = [f'/tmp/create_baseline_flag.csv']

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

    print(f"Waiting for baseline startup flag to appear in {HMS_CREATE_BASELINE_FLAG}...")

    found_files = {key: False for key in file_keys}

    while not all(found_files.values()):
        for key in file_keys:
            if not found_files[key]:
                if file_exists(key):
                    print(f"Found: {key}")
                    found_files[key] = True
        if not all(found_files.values()):
            time.sleep(int(S3_POLLING_INTERVAL))

          
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

def parse_csv_flag(filepath: str, object_name: str):
    """
    Gets as input the local file path and parse it in order to set the timestamp that filters
    the HMS DB in order to create the baseline file accordingly.
     
    filepath: get as input the csv file path from local_paths[0]

        - Read the CSV file
        - Get the first value of the 'backup_time' column
        - Set the global FILTER_TIMESTAMP variable to this value
        - Get the first value of the 'backup_file_name' column
        - Set the global HMS_BASELINE_OBJECT_NAME variable to this value
    Returns:
        FILTER_TIMESTAMP (int): The timestamp value from the CSV file.
        HMS_BASELINE_OBJECT_NAME (str): The baseline object name derived from the CSV file.
    """

    try:
        df = pd.read_csv(filepath)
         
        FILTER_TIMESTAMP = df['backup_time'].iloc[0]
        print( f"Setting HMS DB filtering timestamp to: '{FILTER_TIMESTAMP}'")

        backup_name = df['backup_file_name'].iloc[0]
        HMS_BASELINE_OBJECT_NAME = f"{object_name.removesuffix('.csv')}_for_{backup_name.removesuffix('.tar.gz')}.csv" 
        print( f"Setting HMS DB baseline name to: '{HMS_BASELINE_OBJECT_NAME}'")

        return FILTER_TIMESTAMP,HMS_BASELINE_OBJECT_NAME

    except FileNotFoundError:
        sys.exit(f"Error: File '{filepath}' not found.")
    except pd.errors.EmptyDataError:
        sys.exit("Error: File is empty.")
    except pd.errors.ParserError:
        sys.exit("Error: File could not be parsed as CSV.")

        
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


def quote_ident(name: str, dialect):
    return dialect.identifier_preparer.quote(name)

def get_columns(engine, catalog_name: str, table: str, schema: str = "public"):
    with engine.connect() as conn:

        # we ignore varbinary columns as they cannot be converted to string in Trino and therefore can not be included in the fingerprint
        stmt = text(f"""
            SELECT lower(column_name)  AS column_name
            FROM  {catalog_name}information_schema.columns c 
            WHERE UPPER(c.table_name) = UPPER('{table}')
            AND UPPER(c.table_schema) = UPPER('{schema}')
            AND c.data_type NOT IN ('varbinary')
        """)

        result = conn.execute(stmt)
        return [row[0] for row in result]

def generate_baseline_for_table(engine, table: str, schema: str = "public", filter_timestamp: int = None):
    """
    Generates a baseline fingerprint and row count for a given table using its primary key columns.
    This function connects to the specified database engine, inspects the table to determine its primary key columns,
    and generates a fingerprint by hashing the concatenated text representation of each row, ordered by the primary key.
    If the table contains certain key columns (PART_ID, TBL_ID, or DB_ID), it also joins with the corresponding metadata
    table to include the creation time in the result.
    Args:
        engine: SQLAlchemy engine object used to connect to the database.
        table (str): Name of the table for which to generate the baseline.
        schema (str, optional): Schema name of the table. Defaults to "public".
    Returns:
        List: The result of the executed SQL query, typically containing row count, fingerprint, and optionally the maximum creation time.
    Raises:
        Exception: If there is an error during database connection or SQL execution.
    Notes:
        - The function prints a message if no primary key is found for the table.
        - The function assumes the existence of certain metadata tables (PARTITIONS, TBLS, DBS) for join operations.
    """
    row = None
    if src_engine.dialect.name == 'postgresql':
        catalog_name = ""
    else:
        catalog_name = f"{HMS_TRINO_CATALOG}."

    with src_engine.connect() as conn:

        print (conn.dialect.name)
        # Step 1: Get all columns
        all_columns = get_columns(engine=src_engine, catalog_name=catalog_name, table=table, schema=schema)
        #print(all_columns)
        pk_columns = all_columns  # we use all columns as "pk" so that it is always ordered by all values of the row
        if not pk_columns:
            print(f"No primary key found for table {schema}.{table}")
        else:
            # Step 2: Quote identifiers for safety
            full_table = f"{schema}.{quote_ident(table,dialect=conn.dialect)}"

            print (f"Generating fingerprint for table {full_table} using PK columns: {pk_columns}")

            if src_engine.dialect.name == 'postgresql':
                row_to_text_expr = "row(t.*)::text"
                hash_expr = "md5(string_agg(md5(row_text), ''))"
                order_by_clause = ", ".join("t." + quote_ident(col.upper(),dialect=conn.dialect) for col in pk_columns)
            else:
                format_args = ",".join([f"CAST(t.{quote_ident(col, dialect=conn.dialect)} AS varchar)" for col in all_columns])
                format_str = ",".join(["%s"] * len(all_columns))
                row_to_text_expr = f"format('{format_str}', {format_args})"
                order_by_clause = ", ".join("t." + quote_ident(col,dialect=conn.dialect) for col in pk_columns)
                hash_expr = f"""to_hex(
                                md5(
                                    CAST(
                                        array_join(
                                            array_agg(to_hex(md5(CAST(t.row_text AS varbinary))) ORDER BY {order_by_clause}),
                                            ''
                                        ) AS varbinary
                                    )
                                )
                            )
                            """

            create_time_table_join = ""
            create_time_col = ""
            create_time_col_agg = ""
            if "PART_ID".lower() in all_columns:
                create_time_table_join = f"JOIN {catalog_name}public.\"PARTITIONS\" ct ON ct.\"PART_ID\" = t.\"PART_ID\""
                create_time_col = f', ct."CREATE_TIME" AS create_time'
                create_time_col_agg = f', COALESCE(MAX(t.create_time), 0) AS max_create_time'
            elif "TBL_ID".lower() in all_columns:
                create_time_table_join = f"JOIN {catalog_name}public.\"TBLS\" ct ON ct.\"TBL_ID\" = t.\"TBL_ID\""
                create_time_col = f', ct."CREATE_TIME" AS create_time'
                create_time_col_agg = f', COALESCE(MAX(t.create_time), 0) AS max_create_time'
            # DBS only has a create_time column in HMS 4.x
            #elif "DB_ID".lower() in all_columns:
            #    create_time_table_join = f"JOIN {catalog_name}public.\"DBS\" ct ON ct.\"DB_ID\" = t.\"DB_ID\""
            #    create_time_col = f', ct."CREATE_TIME" AS create_time'
            #    create_time_col_alias = f', MAX(t.create_time) AS max_create_time'
            # CTLGS only has a create_time column in HMS 4.x
            #elif "CTLGS_ID".lower() in all_columns:
            #    create_time_table_join = f"JOIN {catalog_name}public.\"CTLGS\" ct ON ct.\"CTLGS_ID\" = t.\"CTLGS_ID\""
            #    create_time_col = f', ct."CREATE_TIME" AS create_time'
            #    create_time_col_alias = f', MAX(t.create_time) AS max_create_time'

            timestamp_where_clause = ""
            if filter_timestamp and create_time_col:
                timestamp_where_clause = f'WHERE ct."CREATE_TIME" <= {filter_timestamp}'

            # Step 3: Prepare and execute SQL
            query = text(f"""
                SELECT COUNT(*) AS row_count
                , {hash_expr} AS fingerprint
                {create_time_col_agg}
                FROM (
                    SELECT {order_by_clause}
                    , {row_to_text_expr} AS row_text
                    {create_time_col}
                    FROM {catalog_name}{full_table} t
                    {create_time_table_join}
                    {timestamp_where_clause}
                    ORDER BY {order_by_clause}
                ) AS t  
            """)

            if (table == 'partitions'):
               print (query)

            logger.debug(f"Executing SQL: {query}")
        
            result = conn.execute(query)
            row = result.fetchone()
        return row

def create_baseline():
    """
    Generates a baseline CSV file containing metadata for a set of tables and optionally uploads it to S3.
    This function retrieves a list of table names, filters out specific system or internal tables, and for each remaining table,
    generates a baseline consisting of the row count, a fingerprint, and a timestamp. The results are written to a CSV file.
    If S3 upload is enabled, the CSV file is uploaded to a specified S3 bucket.
    Returns:
        None
    Side Effects:
        - Writes a CSV file named "hms_baseline.csv" to the local filesystem.
        - Optionally uploads the CSV file to an S3 bucket if S3_UPLOAD_ENABLED is True.
        - Logs upload actions using the logger.
    Dependencies:
        - Assumes the existence of functions and variables: get_table_names, FILTER_TABLES, src_engine,
          generate_baseline_for_table, S3_UPLOAD_ENABLED, logger, HMS_BASELINE_OBJECT_NAME, S3_ADMIN_BUCKET, s3.
    """
    tables = get_table_names(engine=src_engine, catalog_name=catalog_name)

    with open(HMS_BASELINE_FILE_NAME, "w") as f:
        # Print CSV header
        print("table_name,count,fingerprint,timestamp", file=f)

        for table in tables:
            with src_engine.connect() as conn:
                baseline = generate_baseline_for_table(src_engine, table, "public", filter_timestamp=FILTER_TIMESTAMP)

            if baseline is not None:
                count = baseline[0]
                if baseline[1] is not None:
                    fingerprint = baseline[1]
                else:
                    fingerprint = ''
                timestamp = baseline[2] if len(baseline) > 2 else 0
                print(f"{table},{count},{fingerprint},{timestamp}", file=f)

if RUN_AS_CRONJOB:
    wait_for_files()
    download_files()
    FILTER_TIMESTAMP,HMS_BASELINE_OBJECT_NAME = parse_csv_flag(local_files[0],HMS_BASELINE_OBJECT_NAME)
    
    vars_for_next_step = {
       "FILTER_TIMESTAMP": f'{FILTER_TIMESTAMP}',
       "HMS_BASELINE_OBJECT_NAME": f'{HMS_BASELINE_OBJECT_NAME}'
    }
    
    scenario = Scenario()
    scenario.set_scenario_variables(**vars_for_next_step)
    

create_baseline()

# upload the file to S3 to make it available
if S3_UPLOAD_ENABLED:
    logger.info(f"Uploading {HMS_BASELINE_FILE_NAME} to s3://{S3_ADMIN_BUCKET}/{HMS_BASELINE_OBJECT_NAME}")

    s3.upload_file(HMS_BASELINE_FILE_NAME, S3_ADMIN_BUCKET, HMS_BASELINE_OBJECT_NAME)

if RUN_AS_CRONJOB:
    #cleans up the flag to avoid loop
    cleanup()
