"""
This module provides utilities and pytest-based tests for reconciling partition counts and fingerprints
between S3 and Hive Metastore (HMS) tables. It is designed to run both inside and outside Dataiku DSS scenarios,
supporting dynamic configuration via scenario variables or environment variables.
Key functionalities:
- Securely retrieves configuration parameters and credentials from Dataiku scenario variables, Dataiku secrets, or environment variables.
- Establishes connections to HMS via PostgreSQL or Trino, and to S3 (MinIO or AWS).
- Loads baseline partition data and S3 location lists from CSV files stored in S3.
- Queries HMS for partition counts and names, supporting filtering by database, table, and batch.
- Compares partition counts and fingerprints (SHA256 hashes of partition names) between S3 baseline and HMS.
- Provides pytest parameterized tests for validating partition counts and fingerprints for each S3 location.
Functions:
- get_param(name, default): Retrieves configuration parameters from scenario variables or environment variables.
- get_credential(name, default): Retrieves secret credentials from Dataiku secrets or environment variables.
- get_s3_location_list(batch): Loads and optionally filters S3 location list from a CSV file in S3.
- get_s3_partitions_baseline(): Loads baseline partition data from a CSV file in S3 and filters by S3 location list.
- get_latest_timestamp(db_baseline): Returns the latest timestamp from the baseline DataFrame.
- get_hms_partitions_count_and_partnames(s3_location, end_timestamp, filter_database, filter_tables): Queries HMS for partition counts and names for a given S3 location.
- quote_ident(name, dialect): Quotes SQL identifiers for the given dialect.
Pytest tests:
- test_partition_counts(s3_location): Asserts that partition counts in HMS match those in the S3 baseline for each location.
- test_partition_fingerprints(s3_location): Asserts that partition fingerprints in HMS match those in the S3 baseline for each location.
Environment variables and scenario parameters:
- FILTER_DATABASE, FILTER_TABLES, FILTER_BATCH: Filtering options for baseline and HMS queries.
- HMS_DB_ACCESS_STRATEGY: Selects HMS connection method (PostgreSQL or Trino).
- HMS_DB_USER, HMS_DB_PASSWORD, HMS_DB_HOST, HMS_DB_PORT, HMS_DB_NAME: HMS database connection parameters.
- HMS_TRINO_USER, HMS_TRINO_PASSWORD, HMS_TRINO_HOST, HMS_TRINO_PORT, HMS_TRINO_CATALOG, HMS_TRINO_USE_SSL: Trino connection parameters.
- S3_ENDPOINT_URL, S3_ADMIN_BUCKET, S3_BASELINE_OBJECT_NAME, S3_LOCATION_LIST_OBJECT_NAME: S3 connection and object parameters.
- AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY: S3 credentials.
Logging:
- Logs configuration, connection status, and test progress.
Exceptions:
- Raises ValueError if required CSV files from S3 are empty.
"""
import hashlib
import io
import os
import sys

import boto3
import pandas as pd
import pytest
import logging
from sqlalchemy import create_engine, inspect, text
from datetime import datetime, timezone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, get_s3_location_list, get_partition_info

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZONE = get_zone_name(upper=True)

# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=True)
FILTER_DATABASE = get_param('FILTER_DATABASE', "")
#FILTER_TABLES = get_param('FILTER_TABLES', "")
FILTER_BATCH = get_param('FILTER_BATCH', "")
FILTER_STAGE = get_param('FILTER_STAGE', "")

# either postgresql or trino
HMS_DB_ACCESS_STRATEGY = get_param('HMS_DB_ACCESS_STRATEGY', 'postgresql')
# use timestamp from baseline file or current time
USE_BASELINE_TIMESTAMP = get_param('USE_BASELINE_TIMESTAMP', 'false').lower() in ('true', '1', 't')

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
ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_ADMIN_BUCKET = replace_vars_in_string(S3_ADMIN_BUCKET, { "zone": ZONE.upper(), "env": ENV.upper() } )

S3_BASELINE_OBJECT_NAME = get_param('S3_BASELINE_OBJECT_NAME', 'baseline_s3.csv')
S3_BASELINE_OBJECT_NAME = replace_vars_in_string(S3_BASELINE_OBJECT_NAME, { "database": FILTER_DATABASE.upper(), "zone": ZONE.upper(), "env": ENV.upper() } )

S3_LOCATION_LIST_OBJECT_NAME = get_param('S3_LOCATION_LIST_OBJECT_NAME', 's3_locations.csv')
S3_LOCATION_LIST_OBJECT_NAME = replace_vars_in_string(S3_LOCATION_LIST_OBJECT_NAME, { "database": FILTER_DATABASE.upper(), "zone": ZONE.upper(), "env": ENV.upper() } )

# Construct connection URLs
hms_db_url = f'postgresql://{HMS_DB_USER}:{HMS_DB_PASSWORD}@{HMS_DB_HOST}:{HMS_DB_PORT}/{HMS_DB_DBNAME}'
# Construct connection URLs
hms_trino_url = f'trino://{HMS_TRINO_USER}:{HMS_TRINO_PASSWORD}@{HMS_TRINO_HOST}:{HMS_TRINO_PORT}/{HMS_TRINO_CATALOG}'
if HMS_TRINO_USE_SSL:
    hms_trino_url = f'{hms_trino_url}?protocol=https&verify=false'

# Setup connections to the metadatastore, either directly to postgresql or via trino
if HMS_DB_ACCESS_STRATEGY.lower() == 'postgresql':
    src_engine = create_engine(hms_db_url)
else:
    src_engine = create_engine(hms_trino_url)

logger.info(f"Connected using SQLAlchemy dialect: {src_engine.dialect.name}")

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

def get_s3_partitions_baseline(s3_baseline_object_name: str):
    """
    Reads a CSV file from an S3 response object, decodes its content, and loads it into a pandas DataFrame.
    The function expects a global variable `response` containing the S3 response with a 'Body' attribute.
    It decodes the CSV content from bytes to string, checks for empty content, and parses it using pandas.
    Returns:
        pd.DataFrame: DataFrame containing the CSV data from S3.
    Raises:
        ValueError: If the CSV file is empty.
    """

    # Read the object
    response = s3.get_object(Bucket=S3_ADMIN_BUCKET, Key=s3_baseline_object_name)

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

    db_baseline = pd.read_csv(csv_buffer)

    s3_location_list = get_s3_location_list(s3, S3_ADMIN_BUCKET, S3_LOCATION_LIST_OBJECT_NAME, FILTER_BATCH, FILTER_STAGE)
    db_baseline = db_baseline[db_baseline["fully_qualified_table_name"].isin(s3_location_list["fully_qualified_table_name"])]

    return db_baseline

def get_latest_timestamp(db_baseline):
    """
    Returns the latest timestamp from the 'timestamp' column in the provided DataFrame.
    Args:
        db_baseline (pandas.DataFrame): DataFrame containing a 'timestamp' column.
    Returns:
        The maximum value found in the 'timestamp' column, representing the latest timestamp.
    """
    latest_timestamp = db_baseline['timestamp'].max()

    return latest_timestamp

def get_latest_timestamp_for_location(db_baseline, s3_location: str):
    """
    Returns the latest timestamp for a S3 location from the 'timestamp' column in the provided DataFrame.
    Args:
        db_baseline (pandas.DataFrame): DataFrame containing a 'timestamp' column.
    Returns:
        The maximum value found in the 'timestamp' column, representing the latest timestamp.
    """
    latest_timestamp = db_baseline[db_baseline["s3_location"] == s3_location]['timestamp'].max()

    return latest_timestamp

def get_hms_partitions_count_and_partnames(s3_location: str, end_timestamp: int):
    """
    Retrieves the count and names of partitions for a Hive Metastore table located at the specified S3 location,
    with partition creation times up to the given end timestamp.
    The function adapts its SQL query based on the database dialect (PostgreSQL or others) to aggregate partition names.
    It joins relevant Hive Metastore tables to filter by S3 location and returns a single result containing table name,
    table type, partition count, and a comma-separated list of partition names.
    Args:
        s3_location (str): The S3 location of the Hive Metastore table to query.
        end_timestamp (int): The upper bound (inclusive) for partition creation times (UNIX timestamp).
    Returns:
        Mapping or None: A mapping object with keys "TBL_NAME", "TBL_TYPE", "partition_count", and "part_names",
        or None if no matching table is found.
    """

    if src_engine.dialect.name == 'postgresql':
        part_names_expr = """string_agg(p."PART_NAME", ',' ORDER BY p."PART_NAME")"""
        catalog_name = ""
    else:
        part_names_expr = """array_join(array_agg(p."PART_NAME" ORDER BY p."PART_NAME"), ',')"""
        catalog_name = HMS_TRINO_CATALOG + "."

    with src_engine.connect() as conn:
        # TODO: Make end_timestamp optional and configure number of seconds to add
        sql = (text(f"""
            SELECT t."TBL_NAME", t."TBL_TYPE", COALESCE(p."partition_count",0) AS partition_count, COALESCE(p."part_names", '') AS part_names
            FROM (
                SELECT t."TBL_ID",
                    t."CREATE_TIME",
                    t."TBL_NAME",
                    t."TBL_TYPE"
                FROM {catalog_name}public."TBLS" t
                JOIN {catalog_name}public."DBS" d ON t."DB_ID" = d."DB_ID"
                JOIN {catalog_name}public."SDS" s ON t."SD_ID" = s."SD_ID"
                WHERE s."LOCATION" = '{s3_location}'                    
            ) t
            LEFT JOIN (
                SELECT p."TBL_ID",
                    COUNT(*) AS partition_count,
                    {part_names_expr}   part_names
                FROM {catalog_name}public."PARTITIONS" p
                WHERE p."CREATE_TIME" < {end_timestamp}                
                GROUP BY p."TBL_ID"
            ) p
            ON t."TBL_ID" = p."TBL_ID"
        """))
        logger.debug("Executing SQL: {sql}")
        result = conn.execute(sql)
        row = result.mappings().one_or_none()  # strict: must return exactly one row
        return row

def quote_ident(name: str, dialect):
    return dialect.identifier_preparer.quote(name)

# retrieve the baseline (optionally only for a certain batch)
db_baseline = get_s3_partitions_baseline(S3_BASELINE_OBJECT_NAME)

# Dynamically get the s3 locations from the baseline file
s3_locations = db_baseline["s3_location"].tolist()
partition_counts = db_baseline.set_index("s3_location")["partition_count"].to_dict()
partition_fingerprint = db_baseline.set_index("s3_location")["fingerprint"].to_dict()

# retrieve the current timestamp as unix timestamp
now_timestamp = datetime.now(timezone.utc).timestamp()

logger.info(f"Testing {len(s3_locations)} S3 locations")

@pytest.mark.parametrize("s3_location", s3_locations)
def test_partition_counts(s3_location: str):
    max_timestamp = get_latest_timestamp_for_location(db_baseline, s3_location) if USE_BASELINE_TIMESTAMP else now_timestamp
    print(f"Using max_timestamp {max_timestamp} for location {s3_location}")
    partition = get_hms_partitions_count_and_partnames(s3_location, max_timestamp)
    assert partition is not None, f"Expected a row for {s3_location} from HMS select query, but got None"

    logger.debug(f"Partition details for {s3_location}: {partition}")

    expected_count: int = partition_counts[s3_location]
    actual_count: int = partition["partition_count"]
    assert expected_count == actual_count, f"Partition count mismatch for {s3_location} in Hive Metastore: expected {expected_count} (S3), but got {actual_count} (HMS), difference of {expected_count - actual_count} at timestamp {max_timestamp}"

@pytest.mark.parametrize("s3_location", s3_locations)
def test_partition_fingerprints(s3_location: str):
    max_timestamp = get_latest_timestamp_for_location(db_baseline, s3_location) if USE_BASELINE_TIMESTAMP else now_timestamp
    partition = get_hms_partitions_count_and_partnames(s3_location, max_timestamp)
    assert partition is not None, f"Expected a row for {s3_location} from HMS select query, but got None"

    logger.debug(f"Partition details for {s3_location}: {partition}")
    fingerprint = ""
    if partition["partition_count"] > 0: 
        fingerprint = hashlib.sha256(partition["part_names"].encode('utf-8')).hexdigest()

        if partition_fingerprint[s3_location] == fingerprint:
            assert True
        else:
            partition_info = get_partition_info(s3, s3_location)
            
            partition_list_s3 = partition_info.get("partition_list", [])
            partition_list_hms = partition["part_names"].split(",") if partition["part_names"] else []
            
            diff_s3 = set(partition_list_s3) - set(partition_list_hms)
            diff_hms = set(partition_list_hms) - set(partition_list_s3)
            
            logger.warning(f"Partitions in S3 but not in HMS: {diff_s3}")
            logger.warning(f"Partitions in HMS but not in S3: {diff_hms}")
            
            assert partition_fingerprint[s3_location] == fingerprint, f"Partition fingerprint mismatch for {s3_location} in Hive Metastore: expected {partition_fingerprint[s3_location]} (S3), but got {fingerprint} (HMS) at timestamp {max_timestamp}. Partitions in S3 but not in HMS: {diff_s3}. Partitions in HMS but not in S3: {diff_hms}"
    else:
        assert True    