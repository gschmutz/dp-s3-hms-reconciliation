import boto3
import os
import hashlib
import logging
from datetime import datetime
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse
from sqlalchemy import create_engine, select, text, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

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
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', 'admin123')

S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_BASELINE_OBJECT_NAME = get_param('S3_BASELINE_OBJECT_NAME', 'baseline_s3.csv')

if HMS_DB_ACCESS_STRATEGY.lower() == 'postgresql':
    # Construct connection URLs
    hms_db_url = f'postgresql://{HMS_DB_USER}:{HMS_DB_PASSWORD}@{HMS_DB_HOST}:{HMS_DB_PORT}/{HMS_DB_DBNAME}'
    
    src_engine = create_engine(hms_db_url)
else:
    hms_trino_url = f'trino://{HMS_TRINO_USER}:{HMS_TRINO_PASSWORD}@{HMS_TRINO_HOST}:{HMS_TRINO_PORT}/{HMS_TRINO_CATALOG}'
    if HMS_TRINO_USE_SSL:
        hms_trino_url = f'{hms_trino_url}?protocol=https&verify=false'
    
    src_engine = create_engine(hms_trino_url)


# Create S3 client configuration
s3_config = {"service_name": "s3"}
if S3_ENDPOINT_URL:
    s3_config["endpoint_url"] = S3_ENDPOINT_URL
if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    s3_config["aws_access_key_id"] = AWS_ACCESS_KEY
    s3_config["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY   

s3 = boto3.client(**s3_config)

Base = declarative_base()

class S3Location(Base):
    __tablename__ = 'users'  # maps to the "users" table in your database

    fully_qualified_table_name = Column(String, primary_key=True)
    database_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    table_type = Column(String, nullable=False)
    location = Column(String, nullable=False)
    has_partitions = Column(String, nullable=False)
    partition_count = Column(Integer, nullable=False)

    def __repr__(self):
        return f"<S3Location(database_name={self.database_name}, table_name='{self.table_name}', table_type='{self.table_type}', location='{self.location}', has_partitions='{self.has_partitions}', partition_count={self.partition_count}, row_num={self.row_num}, bucket={self.bucket})>"

def get_s3_locations_for_tables(filter_database=None, filter_tables=None):
    """
    """

    if src_engine.dialect.name == 'postgresql':
        catalog_name = ""
    else:
        catalog_name = f"{HMS_TRINO_CATALOG}."

    if filter_database and filter_tables:
        filter_where_clause = f"WHERE d.\"NAME\" = '{filter_database}' AND t.\"TBL_NAME\" IN ({filter_tables})"
    elif filter_database:
        filter_where_clause = f"WHERE d.\"NAME\" = '{filter_database}'"
    elif filter_tables:
        filter_where_clause = f"WHERE t.\"TBL_NAME\" IN ({filter_tables})"
    else:
        filter_where_clause = ""    

    with src_engine.connect() as conn:
        Session = sessionmaker(bind=src_engine)
        session = Session()

        stmt = select(S3Location).from_statement(text(f"""
            SELECT
                CONCAT(d."NAME", '.', t."TBL_NAME") as fully_qualified_table_name,
                d."NAME" as database_name,
                t."TBL_NAME" as table_name,
                t."TBL_TYPE" as table_type,
                s."LOCATION" as location,
                CASE
                    WHEN pk.has_partitions >= 1 then 'Y'
                    ELSE 'N'
                END as has_partitions,
                COUNT(p."PART_ID") as partition_count
            FROM
                {catalog_name}public."TBLS" t
            JOIN public."DBS" d on
                t."DB_ID" = d."DB_ID"
            JOIN {catalog_name}public."SDS" s on
                t."SD_ID" = s."SD_ID"
            LEFT JOIN (
                SELECT
                    pk."TBL_ID",
                    GREATEST(SIGN(COUNT(*)), 0) as has_partitions
                FROM
                    {catalog_name}public."PARTITION_KEYS" pk
                GROUP BY
                    pk."TBL_ID") pk on
                t."TBL_ID" = pk."TBL_ID"
            LEFT JOIN {catalog_name}public."PARTITIONS" p on
                t."TBL_ID" = p."TBL_ID"
            {filter_where_clause}
            GROUP BY
                d."NAME",
                t."TBL_NAME",
                t."TBL_TYPE",
                s."LOCATION",
                pk.has_partitions
        """))
        s3_locations = session.execute(stmt).scalars().all()
        return s3_locations

def get_partition_info(s3a_url):
    # Convert s3a:// to s3://
    s3_url = s3a_url.replace("s3a://", "s3://")
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    print(f"Analyzing S3 location: bucket={bucket}, prefix={prefix}")

    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    partitions = set()
    latest_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Detect partition-style folder structure like col=value
            parts = key[len(prefix):].strip("/").split("/")
            partition_parts = [p for p in parts if "=" in p]
            if partition_parts:
                partitions.add("/".join(partition_parts))
            if obj["LastModified"] > latest_ts:
                latest_ts = obj["LastModified"]                

    sorted_partitions = sorted(partitions)
    joined = ",".join(sorted_partitions)
    fingerprint = hashlib.sha256(joined.encode('utf-8')).hexdigest()
                      
    return {
        "s3_location": s3a_url,
        "partition_count": len(partitions),
        "fingerprint": fingerprint,
        "timestamp": int(latest_ts.timestamp())
    }

with open(S3_BASELINE_OBJECT_NAME, "w") as f:
    # Print CSV header
    print("fully_qualified_table_name,database_name,table_name,s3_location,partition_count,fingerprint,timestamp", file=f)

    print(f"Getting S3 locations for tables with filter database='{FILTER_DATABASE}' and filter tables='{FILTER_TABLES}'")
                
    s3_locations = get_s3_locations_for_tables(FILTER_DATABASE, FILTER_TABLES)

    # Iterate through Hive tables
    for s3_location in s3_locations:
        
        info = get_partition_info(f"{s3_location.location}")
        print(f"{s3_location.fully_qualified_table_name},{s3_location.database_name},{s3_location.table_name},{info['s3_location']},{info['partition_count']},{info['fingerprint']},{info['timestamp']}", file=f)

# upload the file to S3 to make it available
if S3_UPLOAD_ENABLED:
    logger.info(f"Uploading {S3_BASELINE_OBJECT_NAME} to s3://{S3_ADMIN_BUCKET}/{S3_BASELINE_OBJECT_NAME}")

    s3.upload_file(S3_BASELINE_OBJECT_NAME, S3_ADMIN_BUCKET, S3_BASELINE_OBJECT_NAME)