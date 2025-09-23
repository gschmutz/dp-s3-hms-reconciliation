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
import time

import boto3
import pandas as pd
import pytest
import logging
from sqlalchemy import create_engine, inspect, text

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
    return_value = default
    if scenario is not None:
        return_value = scenario.get_all_variables().get(name, default)
    else:
        return_value = os.getenv(name, default)

    logger.info(f"{name}: {return_value}")

    return return_value
 
def get_credential(name, default=None) -> str:
    """
    Retrieves the value of a secret credential by its name.
    Args:
        name (str): The key name of the credential to retrieve.
        default (str, optional): The default value to return if the credential is not found. Defaults to None.
    Returns:
        str: The value of the credential if found, otherwise the default value.
    """
    return_value = default
    if client is not None:
        secrets = client.get_auth_info(with_secrets=True)["secrets"]
        for secret in secrets:
            if secret["key"] == name:
                if "value" in secret:
                    return_value = secret["value"]
                else:
                    break
    else:
        return_value = os.getenv(name, default)
    logger.info(f"{name}: *****")
         
    return return_value

# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
FILTER_DATABASE = get_param('FILTER_DATABASE', "")
FILTER_TABLES = get_param('FILTER_TABLES', "")
FILTER_BATCH = get_param('FILTER_BATCH', "")

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
BASELINE_OBJECT_KEY = get_param('S3_BASELINE_OBJECT_NAME', 'baseline_s3.csv')
S3_LOCATION_LIST_OBJECT_NAME = get_param('S3_LOCATION_LIST_OBJECT_NAME', 's3_locations.csv')

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

def get_s3_location_list(batch: str) -> pd.DataFrame:
    """
    Retrieves a list of S3 locations from a CSV file stored in an S3 bucket and returns it as a pandas DataFrame.
    Parameters:
        batch (str): The name of the batch to filter the locations by. If provided, only locations matching this batch are returned.
    Returns:
        pd.DataFrame: A DataFrame containing the S3 location list, optionally filtered by the specified batch.
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
    if batch:
        s3_location_list = s3_location_list[s3_location_list["batch"] == int(batch)]
        print(s3_location_list)

    return s3_location_list

def get_s3_partitions_baseline():
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
    response = s3.get_object(Bucket=S3_ADMIN_BUCKET, Key=BASELINE_OBJECT_KEY)

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
    
    s3_location_list = get_s3_location_list(FILTER_BATCH)
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

def get_hms_partitions_count_and_partnames(s3_location: str, end_timestamp: int, filter_database=None, filter_tables=None):
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

    if filter_database and filter_tables:
        filter_where_clause = f"AND d.\"NAME\" = '{filter_database}' AND t.\"TBL_NAME\" IN ({filter_tables})"
    elif filter_database:
        filter_where_clause = f"AND d.\"NAME\" = '{filter_database}'"
    elif filter_tables:
        filter_where_clause = f"AND t.\"TBL_NAME\" IN ({filter_tables})"
    else:
        filter_where_clause = ""    

    with src_engine.connect() as conn:
        # TODO: Make end_timestamp optional and configure number of seconds to add
        result = conn.execute(text(f"""
            SELECT t."TBL_NAME", t."TBL_TYPE", COALESCE(p."partition_count",0) AS partition_count, p."part_names"
            FROM (
                SELECT p."TBL_ID",
                    COUNT(*) AS partition_count,
                    {part_names_expr}   part_names
                FROM {catalog_name}public."PARTITIONS" p
                WHERE p."CREATE_TIME" <= {end_timestamp} + 1                 
                GROUP BY p."TBL_ID"
            ) p
            RIGHT JOIN (
                SELECT t."TBL_ID",
                    t."CREATE_TIME",
                    t."TBL_NAME",
                    t."TBL_TYPE"
                FROM {catalog_name}public."TBLS" t
                JOIN {catalog_name}public."DBS" d ON t."DB_ID" = d."DB_ID"
                JOIN {catalog_name}public."SDS" s ON t."SD_ID" = s."SD_ID"
                WHERE s."LOCATION" = '{s3_location}'
                {filter_where_clause}
            ) t
            ON t."TBL_ID" = p."TBL_ID"
        """))
        row = result.mappings().one_or_none()  # strict: must return exactly one row
        return row

def quote_ident(name: str, dialect):
    return dialect.identifier_preparer.quote(name)

# retrieve the baseline (optionally only for a certain batch)
db_baseline = get_s3_partitions_baseline()

# Dynamically get the s3 locations from the baseline file
s3_locations = db_baseline["s3_location"].tolist()
partition_counts = db_baseline.set_index("s3_location")["partition_count"].to_dict()
partition_fingerprint = db_baseline.set_index("s3_location")["fingerprint"].to_dict()

# set the timestamp to use for the query from HMS
if not USE_BASELINE_TIMESTAMP:
    max_timestamp = get_latest_timestamp(db_baseline)
else:
    max_timestamp = int(time.time())

logger.info(f"Testing {len(s3_locations)} S3 locations with max timestamp {max_timestamp}")

@pytest.mark.parametrize("s3_location", s3_locations)
def test_partition_counts(s3_location: str):
    partition = get_hms_partitions_count_and_partnames(s3_location, max_timestamp, FILTER_DATABASE, FILTER_TABLES)
    assert partition is not None, f"Expected a row for {s3_location} from HMS select query, but got None"
    expected_count: int = partition_counts[s3_location]
    actual_count: int = partition["partition_count"]
    assert expected_count == actual_count, f"Partition count mismatch for {s3_location} in Hive Metastore: expected {expected_count} (S3), but got {actual_count} (HMS)"

@pytest.mark.parametrize("s3_location", s3_locations)
def test_partition_fingerprints(s3_location: str):
    partition = get_hms_partitions_count_and_partnames(s3_location, max_timestamp, FILTER_DATABASE, FILTER_TABLES)
    assert partition is not None, f"Expected a row for {s3_location} from HMS select query, but got None"

    # part_names is a comma-separated string of partition names
    fingerprint = hashlib.sha256(partition["part_names"].encode('utf-8')).hexdigest()

    assert partition_fingerprint[s3_location] == fingerprint, f"Partition fingerprint mismatch for {s3_location} in Hive Metastore: expected {partition_fingerprint[s3_location]} (S3), but got {fingerprint} (HMS)"