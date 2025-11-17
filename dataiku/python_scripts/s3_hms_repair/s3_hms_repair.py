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
import sys
import logging
import boto3
import pandas as pd
from pyhive import hive
from typing import Optional
from sqlalchemy import create_engine, text
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, get_s3_location_list

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
ZONE = get_zone_name(upper=True)
# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=True)

FILTER_DATABASE = get_param('FILTER_DATABASE', None)
FILTER_TABLES = get_param('FILTER_TABLES', None)
FILTER_BATCH = get_param('FILTER_BATCH', "")                    # either all or a specific batch number, if empty it will not use the batch filter at all
FILTER_STAGE = get_param('FILTER_STAGE', "")                    # either all or a specific stage number, if empty it will not use the stage filter at all

DRY_RUN = get_param('DRY_RUN', 'true').lower() in ('true', '1', 't')

# either postgresql or trino
HMS_VERSION = get_param('HMS_VERSION', '3')                       # either "3" or "4"

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

HMS_HOST = get_param('HMS_HOST', 'hive-server')
HMS_PORT = get_param('HMS_PORT', '10000')
HMS_USER = get_param('HMS_USER', 'hive')
HMS_PASSWORD = get_param('HMS_PASSWORD', 'abc123!')

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

# Connect to MinIO or AWS S3
ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_ADMIN_BUCKET = replace_vars_in_string(S3_ADMIN_BUCKET, { "zone": ZONE.upper(), "env": ENV.upper() } )
S3_ADMIN_BUCKET_PREFIX = get_param('S3_ADMIN_BUCKET_PREFIX', '')
S3_LOCATION_LIST_OBJECT_NAME = get_param('S3_LOCATION_LIST_OBJECT_NAME', 's3_locations.csv')
S3_LOCATION_LIST_OBJECT_NAME = replace_vars_in_string(S3_LOCATION_LIST_OBJECT_NAME, { "admin-bucket-prefix": S3_ADMIN_BUCKET_PREFIX, "database": FILTER_DATABASE.upper(), "zone": ZONE.upper(), "env": ENV.upper() } )

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
    s3_config["verify"] = False  # Disable SSL verification for self-signed certificates

s3 = boto3.client(**s3_config)

def get_tables(filter_database: Optional[str] = None, filter_tables: Optional[list[str]] = None) -> pd.DataFrame:
    if hms_engine.dialect.name == 'postgresql':
        catalog_name = ""
    else:
        catalog_name = f"{HMS_TRINO_CATALOG}."

    if filter_database and filter_tables:
        filter_tables_str = ",".join([f"'{tbl}'" for tbl in filter_tables])
        filter_where_clause = f"WHERE d.\"NAME\" = '{filter_database}' AND t.\"TBL_NAME\" IN ({filter_tables_str})"
    elif filter_database:
        filter_where_clause = f"WHERE d.\"NAME\" = '{filter_database}'"
    elif filter_tables:
        filter_tables_str = ",".join([f"'{tbl}'" for tbl in filter_tables])
        filter_where_clause = f"WHERE t.\"TBL_NAME\" = '{filter_tables}'"
    else:
        filter_where_clause = ""            

    with hms_engine.connect() as conn:
        # TODO: Make end_timestamp optional and configure number of seconds to add
        sql = f"""
            SELECT CONCAT(d."NAME", '.', t."TBL_NAME") as fully_qualified_table_name, 
               d."NAME" as "DATABASE_NAME",
               t."TBL_ID",
               t."CREATE_TIME",
               t."TBL_NAME",
               t."TBL_TYPE",
               CASE
                    WHEN COALESCE(pk.has_partitions, 0) >= 1 then 'Y'
                    ELSE 'N'
               END as has_partitions
            FROM {catalog_name}public."TBLS" t
            JOIN {catalog_name}public."DBS" d ON t."DB_ID" = d."DB_ID"
            LEFT JOIN (
                SELECT
                    pk."TBL_ID",
                    GREATEST(SIGN(COUNT(*)), 0) as has_partitions
                FROM
                    {catalog_name}public."PARTITION_KEYS" pk
                GROUP BY
                    pk."TBL_ID") pk 
                ON t."TBL_ID" = pk."TBL_ID"            
            {filter_where_clause}
        """

        result = conn.execute(text(sql))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

def do_trino_repair(filter_database: Optional[str] = None, filter_tables: Optional[list[str]] = None, filter_batch: Optional[str] = None, filter_stage: Optional[str] = None):
    filtered_tables = get_tables(filter_database, filter_tables)

    logger.info(f"Number of tables: {len(filtered_tables)}")
    
    if filter_batch or filter_stage:
        s3_location_list = get_s3_location_list(s3, S3_ADMIN_BUCKET, S3_LOCATION_LIST_OBJECT_NAME, filter_batch, filter_stage)
        logger.info(f"Number of s3 location entries: {len(s3_location_list)}")

        filtered_tables = filtered_tables[filtered_tables["fully_qualified_table_name"].isin(s3_location_list["fully_qualified_table_name"])]

    # Loop over each row in the filtered_tables DataFrame
    for _, table in filtered_tables.iterrows():
        table_name = table["TBL_NAME"]
        database = table["DATABASE_NAME"]
        has_partitions = table["has_partitions"]
       
        # apply filters it set
        if filter_database and database.lower() != filter_database.lower():
            continue
        if filter_tables and table_name.lower() not in filter_tables:
            continue

        with trino_engine.connect() as conn:
            if has_partitions == "N":
                logger.info(f"Skipping table {database}.{table_name} as it has no partitions")
                continue
            if not DRY_RUN:
                logger.info(f"Executing repairing table {database}.{table_name} via Trino") 
                conn.execute(text(f"call {TRINO_CATALOG}.system.sync_partition_metadata('{database}', '{table_name}', 'FULL')"))
            else:
                logger.info(f"DRY RUN - would execute repairing table {database}.{table_name} via Trino")

def do_hms_3x_repair(filter_database: Optional[str] = None, filter_tables: Optional[list[str]] = None, filter_batch: Optional[str] = None, filter_stage: Optional[str] = None):
    filtered_tables = get_tables(filter_database, filter_tables)

    logger.info(f"Number of tables: {len(filtered_tables)}")

    if filter_batch or filter_stage:
        s3_location_list = get_s3_location_list(s3, S3_ADMIN_BUCKET, S3_LOCATION_LIST_OBJECT_NAME,filter_batch, filter_stage)
        logger.info(f"Number of s3 location entries: {len(s3_location_list)}")

        filtered_tables = filtered_tables[filtered_tables["fully_qualified_table_name"].isin(s3_location_list["fully_qualified_table_name"])]

    conn = hive.Connection(host=HMS_HOST, port=HMS_PORT, database="default")

    # Loop over each row in the filtered_tables DataFrame
    for _, table in filtered_tables.iterrows():

        table_name = table["TBL_NAME"]
        database = table["DATABASE_NAME"]
        has_partitions = table["has_partitions"]

        # apply filters it set
        if filter_database and database.lower() != filter_database.lower():
            continue
        if filter_tables and table_name.lower() not in filter_tables:
            continue

        # execute MSCK REPAIR
        if has_partitions == "N":
            logger.info(f"Skipping table {database}.{table_name} as it has no partitions")
            continue
        if not DRY_RUN:
            logger.info(f"Executing repairing table {database}.{table_name} via HiveServer2") 

            cursor = conn.cursor()
            cursor.execute(f"MSCK REPAIR TABLE {database}.{table_name}")
            cursor.close()
        else:
            logger.info(f"DRY RUN - would execute repairing table {database}.{table_name} via Trino")
    
    conn.close()

# Convert FILTER_TABLE to a list if it's a comma-separated string and convert to lowercase
filter_tables_list: list[str] = None
if FILTER_TABLES:
    filter_tables_list = [tbl.strip().lower() for tbl in FILTER_TABLES.split(",") if tbl.strip()]

if HMS_VERSION == "3":
    do_hms_3x_repair(FILTER_DATABASE, filter_tables_list, FILTER_BATCH, FILTER_STAGE)
else:
    do_trino_repair(FILTER_DATABASE, filter_tables_list, FILTER_BATCH, FILTER_STAGE)