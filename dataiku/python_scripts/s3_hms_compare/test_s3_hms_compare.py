import hashlib
import io
import os

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
    scenario = None

# will only be used when running inside a scenario in Dataiku
try:
    from dataiku.api_client import api_client
    # This will only succeed if running inside DSS
    client = api_client()
except ImportError:
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

    if client is not None:
        secrets = client.get_auth_info(with_secrets=True)["secrets"]
        return secrets.get(name, default)
    return default

# either postgresql or trino
METASTORE_DB_ACCESS_STRATEGY = get_param('METASTORE_DB_ACCESS_STRATEGY', 'postgresql')

HMS_DB_USER = get_credential('HMS_DB_USER', 'hive')
HMS_DB_PASSWORD = get_credential('HMS_DB_PASSWORD', 'abc123!')
HMS_DB_HOST = get_param('HMS_DB_HOST', 'hive-metastore-db')
HMS_DB_PORT = get_param('HMS_DB_PORT', '5432')
HMS_DB_DBNAME = get_param('HMS_DB_NAME', 'metastore_db')

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_SCHEMA = get_param('TRINO_SCHEMA', 'flight_db')

# Connect to MinIO or AWS S3
ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')

BASELINE_BUCKET_NAME = get_param('S3_BASELINE_BUCKET', 'admin-bucket')
BASELINE_OBJECT_KEY = get_param('S3_BASELINE_OBJECT_NAME', 'baseline_s3.csv')

# Log all variable values (mask passwords)
logger.info(f"METASTORE_DB_ACCESS_STRATEGY: {METASTORE_DB_ACCESS_STRATEGY}")

logger.info(f"HMS_DB_USER: {HMS_DB_USER}")
logger.info(f"HMS_DB_PASSWORD: {'***' if HMS_DB_PASSWORD else ''}")
logger.info(f"HMS_DB_HOST: {HMS_DB_HOST}")
logger.info(f"HMS_DB_PORT: {HMS_DB_PORT}")
logger.info(f"HMS_DB_DBNAME: {HMS_DB_DBNAME}")

logger.info(f"TRINO_USER: {TRINO_USER}")
logger.info(f"TRINO_PASSWORD: {'***' if TRINO_PASSWORD else ''}")
logger.info(f"TRINO_HOST: {TRINO_HOST}")
logger.info(f"TRINO_PORT: {TRINO_PORT}")
logger.info(f"TRINO_CATALOG: {TRINO_CATALOG}")
logger.info(f"TRINO_SCHEMA: {TRINO_SCHEMA}")

logger.info(f"ENDPOINT_URL: {ENDPOINT_URL}")
logger.info(f"BASELINE_BUCKET_NAME: {BASELINE_BUCKET_NAME}")
logger.info(f"BASELINE_OBJECT_KEY: {BASELINE_OBJECT_KEY}")

# Construct connection URLs
postgresql_url = f'postgresql://{HMS_DB_USER}:{HMS_DB_PASSWORD}@{HMS_DB_HOST}:{HMS_DB_PORT}/{HMS_DB_DBNAME}'
# Construct connection URLs
trino_url = f'trino://{TRINO_USER}:{TRINO_PASSWORD}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}/{TRINO_SCHEMA}'

# Setup connections to the metadatastore, either directly to postgresql or via trino
if METASTORE_DB_ACCESS_STRATEGY.lower() == 'postgresql':
    src_engine = create_engine(postgresql_url)
else:
    src_engine = create_engine(trino_url)
    # You can get the dialect name (db type) from the engine

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

s3 = boto3.client(**s3_config)

# Read the object
response = s3.get_object(Bucket=BASELINE_BUCKET_NAME, Key=BASELINE_OBJECT_KEY)

def get_s3_partitions_baseline():
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
    return db_baseline

db_baseline = get_s3_partitions_baseline()

def get_latest_timestamp(db_baseline):
    
    latest_timestamp = db_baseline['timestamp'].max()

    return latest_timestamp

def get_hms_partitions_count_and_partnames(s3_location: str, end_timestamp: int):
    if src_engine.dialect.name == 'postgresql':
        part_names_expr = """string_agg(p."PART_NAME", ',' ORDER BY p."PART_NAME")"""
        catalog_name = ""
    else:
        part_names_expr = """array_join(array_agg(p."PART_NAME" ORDER BY p."PART_NAME"), ',')"""
        catalog_name = "hive_metastore_db."

    with src_engine.connect() as conn:
        # TODO: Make end_timestamp optional and configure number of seconds to add
        result = conn.execute(text(f"""
            SELECT t."TBL_NAME", t."TBL_TYPE", p."partition_count", p."part_names"
            FROM (
                SELECT p."TBL_ID",
                    COUNT(*) AS partition_count,
                    {part_names_expr}   part_names
                FROM {catalog_name}public."PARTITIONS" p
                WHERE p."CREATE_TIME" <= {end_timestamp} + 1                 
                GROUP BY p."TBL_ID"
            ) p
            JOIN (
                SELECT t."TBL_ID",
                    t."CREATE_TIME",
                    t."TBL_NAME",
                    t."TBL_TYPE"
                FROM {catalog_name}public."TBLS" t
                JOIN {catalog_name}public."DBS" d ON t."DB_ID" = d."DB_ID"
                JOIN {catalog_name}public."SDS" s ON t."SD_ID" = s."SD_ID"
                WHERE s."LOCATION" = '{s3_location}'
            ) t
            ON t."TBL_ID" = p."TBL_ID"
        """))
        row = result.mappings().one_or_none()  # strict: must return exactly one row
        return row

def quote_ident(name: str, dialect):
    return dialect.identifier_preparer.quote(name)

# Dynamically get the s3 locations from the baseline file
s3_locations = db_baseline["s3_location"].tolist()
max_timestamp = get_latest_timestamp(db_baseline)
partition_counts = db_baseline.set_index("s3_location")["partition_count"].to_dict()
partition_fingerprint = db_baseline.set_index("s3_location")["fingerprint"].to_dict()

@pytest.mark.parametrize("s3_location", s3_locations)
def test_partition_counts(s3_location):
    partition = get_hms_partitions_count_and_partnames(s3_location, max_timestamp, )
    assert partition is not None, f"Expected a row for {s3_location} from HMS select query, but got None"
    expected_count: int = partition_counts[s3_location]
    actual_count: int = partition["partition_count"]
    assert expected_count == actual_count, f"Partition count mismatch for {s3_location} in Hive Metastore: expected {expected_count} (S3), but got {actual_count} (HMS)"

@pytest.mark.parametrize("s3_location", s3_locations)
def test_partition_fingerprints(s3_location):
    partition = get_hms_partitions_count_and_partnames(s3_location, max_timestamp)
    assert partition is not None, f"Expected a row for {s3_location} from HMS select query, but got None"

    # part_names is a comma-separated string of partition names
    fingerprint = hashlib.sha256(partition["part_names"].encode('utf-8')).hexdigest()

    assert partition_fingerprint[s3_location] == fingerprint, f"Partition fingerprint mismatch for {s3_location} in Hive Metastore: expected {partition_fingerprint[s3_location]} (S3), but got {fingerprint} (HMS)"