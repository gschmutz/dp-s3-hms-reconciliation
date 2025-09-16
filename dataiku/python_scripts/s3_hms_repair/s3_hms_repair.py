"""
This script performs repair operations on Hive Metastore tables by synchronizing partition metadata either via Hive (using MSCK REPAIR TABLE) or Trino (using sync_partition_metadata procedure), depending on the configured HMS version and access strategy.
Main Features:
- Retrieves configuration and credentials from Dataiku scenario variables or environment variables.
- Supports both direct PostgreSQL access and Trino access to the Hive Metastore.
- Filters tables to repair based on optional database and table filters.
- Executes repair operations for all or filtered tables:
    - For HMS version 3: Uses Hive's MSCK REPAIR TABLE command.
    - For HMS version 4 or Trino strategy: Uses Trino's sync_partition_metadata procedure.
Functions:
- get_param: Retrieves parameter values from scenario variables or environment variables.
- get_credential: Retrieves secret credentials from Dataiku client or defaults.
- get_tables: Fetches table metadata from the Hive Metastore database.
- do_trino_repair: Repairs tables using Trino's sync_partition_metadata procedure.
- do_hms_3x_repair: Repairs tables using Hive's MSCK REPAIR TABLE command.
Usage:
Run the script directly to perform repair operations based on the configured HMS version and filters.
"""
import sys
import os
import io
import logging
import boto3
import pandas as pd
from pyhive import hive
from typing import Optional
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
# will only be used when running inside a scenario in Dataiku
try:
    from dataiku.scenario import Scenario
    # This will only succeed if running inside DSS
    scenario = Scenario()
except ImportError:
    logger.info("Unable to setup dataiku scenario API due to import error")    
    scenario = None
 
# will only be used when running inside a scenario in Dataiku
try:
    import dataiku
    # This will only succeed if running inside DSS
    client = dataiku.api_client()
except ImportError:
    logger.info("Unable to setup dataiku client API due to import error")
    client = None
 
def get_param(name, default=None) -> str:
    """
    Retrieves the value of a parameter by name from the scenario variables if available,
    otherwise from the environment variables.
 
    Args:
        name (str): The name of the parameter to retrieve.
        default (Any, optional): The default value to return if the parameter is not found. Defaults to None.
 
    Returns:
        Any: The value of the parameter if found, otherwise the default value.
    """
    if scenario is not None:
        return scenario.get_all_variables().get(name, default)
    return os.getenv(name, default)
 
def get_credential(name, default=None) -> str:
    """
    Retrieves the value of a secret credential by its name.
    Args:
        name (str): The key name of the credential to retrieve.
        default (str, optional): The default value to return if the credential is not found. Defaults to None.
    Returns:
        str: The value of the credential if found, otherwise the default value.
    """
    if client is not None:
        secrets = client.get_auth_info(with_secrets=True)["secrets"]
        for secret in secrets:
            if secret["key"] == name:
                if "value" in secret:
                    return secret["value"]
                else:
                    break
    return default

# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
FILTER_DATABASE = get_param('FILTER_DATABASE', None)
FILTER_TABLE = get_param('FILTER_TABLE', None)
FILTER_BUCKET = get_param('FILTER_BUCKET', "")

# either postgresql or trino
HMS_VERSION = get_param('HMS_VERSION', '3')                       # either "3" or "4"

HMS_HOST = get_param('HMS_HOST', 'hive-metastore')
HMS_PORT = get_param('HMS_PORT', '10000')
HMS_USER = get_credential('HMS_USER', 'hive')
HMS_PASSWORD = get_credential('HMS_PASSWORD', 'abc123!')

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

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

# Connect to MinIO or AWS S3
ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_LOCATION_LIST_OBJECT_NAME = get_param('S3_LOCATION_LIST_OBJECT_NAME', 's3_locations.csv')

# Construct connection URLs
hms_db_url = f'postgresql://{HMS_DB_USER}:{HMS_DB_PASSWORD}@{HMS_DB_HOST}:{HMS_DB_PORT}/{HMS_DB_DBNAME}'

hms_trino_url = f'trino://{HMS_TRINO_USER}:{HMS_TRINO_PASSWORD}@{HMS_TRINO_HOST}:{HMS_TRINO_PORT}/{HMS_TRINO_CATALOG}'
if HMS_TRINO_USE_SSL:
    hms_trino_url = f'{hms_trino_url}?protocol=https&verify=false'

# Setup connections to the metadatastore, either directly to postgresql or via trino
if HMS_DB_ACCESS_STRATEGY.lower() == 'postgresql':
    hms_engine = create_engine(hms_db_url)
else:
    hms_engine = create_engine(hms_trino_url)
    # You can get the dialect name (db type) from the engine

# Construct connection URLs
trino_url = f'trino://{TRINO_USER}:{TRINO_PASSWORD}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}'
if TRINO_USE_SSL:
    trino_url = f'{trino_url}?protocol=https&verify=false'

trino_engine = create_engine(trino_url)

# Create a session and S3 client
s3 = boto3.client('s3')

# Create S3 client configuration
s3_config = {"service_name": "s3"}
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', None)
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', None)

if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    s3_config["aws_access_key_id"] = AWS_ACCESS_KEY
    s3_config["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
if ENDPOINT_URL:
    s3_config["endpoint_url"] = ENDPOINT_URL

s3 = boto3.client(**s3_config)

def get_s3_location_list(bucket: str) -> pd.DataFrame:
    """
    Retrieves a list of S3 locations from a CSV file stored in an S3 bucket and returns it as a pandas DataFrame.
    Parameters:
        bucket (str): The name of the S3 bucket to filter the locations by. If provided, only locations matching this bucket are returned.
    Returns:
        pd.DataFrame: A DataFrame containing the S3 location list, optionally filtered by the specified bucket.
    Raises:
        ValueError: If the CSV file retrieved from S3 is empty.
    Notes:
        - The function expects the CSV file to be accessible via the global S3_ADMIN_BUCKET and S3_LOCATION_LIST_OBJECT_NAME.
        - The function assumes the existence of a global `s3` client and required imports (`io`, `pandas as pd`).
    """

    # Read the object
    response = s3.get_object(Bucket=S3_ADMIN_BUCKET, Key=S3_LOCATION_LIST_OBJECT_NAME)

    # `response['Body'].read()` returns bytes, decode to string
    csv_string = response['Body'].read().decode('utf-8')

    # Debug: print the content
    print(f"CSV content length: {len(csv_string)}")
    print(f"First 100 chars: {csv_string[:100]}")

    # Add error handling
    if not csv_string.strip():
        raise ValueError("CSV file is empty")

    # Wrap the string in a StringIO buffer
    csv_buffer = io.StringIO(csv_string)

    s3_location_list = pd.read_csv(csv_buffer)
    if bucket:
        s3_location_list = s3_location_list[s3_location_list["bucket"] == int(bucket)]
        print(s3_location_list)

    return s3_location_list

def get_tables(database_name: Optional[str] = None):
    if hms_engine.dialect.name == 'postgresql':
        catalog_name = ""
    else:
        catalog_name = f"{HMS_TRINO_CATALOG}."

    with hms_engine.connect() as conn:
        # TODO: Make end_timestamp optional and configure number of seconds to add
        sql = f"""
            SELECT CONCAT(d."NAME", '.', t."TBL_NAME") as fully_qualified_table_name, 
               d."NAME" as "DATABASE_NAME",
               t."TBL_ID",
               t."CREATE_TIME",
               t."TBL_NAME",
               t."TBL_TYPE"
            FROM {catalog_name}public."TBLS" t
            JOIN {catalog_name}public."DBS" d ON t."DB_ID" = d."DB_ID"
        """
        if (database_name is not None) and (database_name != ""):
            sql += f" WHERE d.\"NAME\" = '{database_name}'"

        result = conn.execute(text(sql))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

def do_trino_repair(database_name: Optional[str] = None, filter_database: Optional[str] = None, filter_table: Optional[str] = None):
    s3_location_list = get_s3_location_list(FILTER_BUCKET)
    all_tables = get_tables(database_name)

    filtered_tables = all_tables[all_tables["fully_qualified_table_name"].isin(s3_location_list["fully_qualified_table_name"])]

    # Loop over each row in the filtered_tables DataFrame
    for _, table in filtered_tables.iterrows():

        table_name = table["TBL_NAME"]
        database = table["DATABASE_NAME"]

        # apply filters it set
        if filter_database and database != filter_database:
            continue
        if filter_table and table_name != filter_table:
            continue

        with trino_engine.connect() as conn:
            logger.info(f"Repairing table {database}.{table_name} via Trino")   

            conn.execute(text(f"call minio.system.sync_partition_metadata('{database}', '{table_name}', 'FULL')"))

def do_hms_3x_repair(database_name: Optional[str] = None, filter_database: Optional[str] = None, filter_table: Optional[str] = None):
    s3_location_list = get_s3_location_list(FILTER_BUCKET)
    all_tables = get_tables(database_name)

    filtered_tables = all_tables[all_tables["fully_qualified_table_name"].isin(s3_location_list["fully_qualified_table_name"])]

    conn = hive.Connection(host=HMS_HOST, port=HMS_PORT, database="default")

    # Loop over each row in the filtered_tables DataFrame
    for _, table in filtered_tables.iterrows():

        table_name = table["TBL_NAME"]
        database = table["DATABASE_NAME"]

        # apply filters it set
        if filter_database and database != filter_database:
            continue
        if filter_table and table_name != filter_table:
            continue

        cursor = conn.cursor()
        # execute MSCK REPAIR
        cursor.execute(f"MSCK REPAIR TABLE {database}.{table_name}")

        cursor.close()
    
    conn.close()

if __name__ == "__main__":
    if HMS_VERSION == "3":
        do_hms_3x_repair(None, FILTER_DATABASE, FILTER_TABLE)
    else:
        do_trino_repair(None, FILTER_DATABASE, FILTER_TABLE)