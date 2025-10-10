"""
This script creates a baseline CSV file containing metadata and partition information for tables stored in S3, as referenced by a Hive Metastore (HMS) database. It supports both PostgreSQL and Trino as HMS backends and can run inside or outside Dataiku DSS scenarios.
Main functionalities:
- Retrieves configuration parameters and credentials from Dataiku scenario variables or environment variables.
- Connects to HMS database (PostgreSQL or Trino) to fetch table metadata, including S3 locations.
- For each table, analyzes the corresponding S3 location to determine partition structure, count, fingerprint, and latest modification timestamp.
- Writes the collected information to a CSV file.
- Optionally uploads the CSV file to an S3 bucket.
Key components:
- Parameter and credential retrieval functions (`get_param`, `get_credential`)
- SQLAlchemy ORM mapping for table metadata (`S3Location`)
- Functions to query HMS for S3 locations and analyze S3 partition info
- S3 client configuration supporting MinIO and AWS S3
- Logging for operational visibility
Environment variables and scenario parameters allow filtering by database and table, configuring S3 endpoints, and controlling upload behavior.

"""
import boto3
import os
import sys
import hashlib
import logging
from collections import defaultdict
from urllib.parse import urlparse
from sqlalchemy import create_engine, select, text, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string, get_partition_info

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZONE = get_zone_name(upper=True)

# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=True)
FILTER_DATABASE = get_param('FILTER_DATABASE', "")
FILTER_TABLES = get_param('FILTER_TABLES', "")

HMS_DB_ACCESS_STRATEGY = get_param('HMS_DB_ACCESS_STRATEGY', 'postgresql')

HMS_DB_USER = get_credential('HMS_DB_USER', 'hive')
HMS_DB_PASSWORD = get_credential('HMS_DB_PASSWORD', 'abc123!')
HMS_DB_HOST = get_param('HMS_DB_HOST', '172.26.183.2')
HMS_DB_PORT = get_param('HMS_DB_PORT', '5442')
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
    s3_config["verify"] = False  # Disable SSL verification for self-signed certificates
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
    Retrieves S3 location information for tables from the Hive Metastore (HMS) via SQLAlchemy.
    Queries the metastore to obtain details about tables, including their fully qualified names,
    database names, table names, types, S3 locations, partition status, and partition counts.
    Optional filters can be applied to restrict results to specific databases or tables.
    Args:
        filter_database (str, optional): Name of the database to filter results by. Defaults to None.
        filter_tables (str, optional): Comma-separated list of table names to filter results by. Defaults to None.
    Returns:
        list[S3Location]: A list of S3Location objects containing metadata for each table matching the filters.
    """

    if src_engine.dialect.name == 'postgresql':
        catalog_name = ""
    else:
        catalog_name = f"{HMS_TRINO_CATALOG}."

    if filter_database and filter_tables:
        filter_tables_str = ",".join([f"'{tbl.strip()}'" for tbl in filter_tables.split(",")])
        filter_where_clause = f"WHERE d.\"NAME\" = '{filter_database}' AND t.\"TBL_NAME\" IN ({filter_tables_str})"
    elif filter_database:
        filter_where_clause = f"WHERE d.\"NAME\" = '{filter_database}'"
    elif filter_tables:
        filter_tables_str = ",".join([f"'{tbl.strip()}'" for tbl in filter_tables.split(",")])
        filter_where_clause = f"WHERE t.\"TBL_NAME\" IN ({filter_tables_str})"
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
                    WHEN COALESCE(pk.has_partitions, 0) >= 1 then 'Y'
                    ELSE 'N'
                END as has_partitions,
                COUNT(p."PART_ID") as partition_count
            FROM
                {catalog_name}public."TBLS" t
            JOIN public."DBS" d 
                ON t."DB_ID" = d."DB_ID"
            JOIN {catalog_name}public."SDS" s 
                ON t."SD_ID" = s."SD_ID"
            LEFT JOIN (
                SELECT
                    pk."TBL_ID",
                    GREATEST(SIGN(COUNT(*)), 0) as has_partitions
                FROM
                    {catalog_name}public."PARTITION_KEYS" pk
                GROUP BY
                    pk."TBL_ID") pk 
                ON t."TBL_ID" = pk."TBL_ID"
            LEFT JOIN {catalog_name}public."PARTITIONS" p 
                ON t."TBL_ID" = p."TBL_ID"
            {filter_where_clause}
            GROUP BY
                d."NAME",
                t."TBL_NAME",
                t."TBL_TYPE",
                s."LOCATION",
                pk.has_partitions
        """))

        logger.debug(f"Executing SQL: {stmt}")
        
        s3_locations = session.execute(stmt).scalars().all()
        return s3_locations

S3_BASELINE_OBJECT_NAME = replace_vars_in_string(S3_BASELINE_OBJECT_NAME, { "database": FILTER_DATABASE.upper(), "zone": ZONE.upper(), "env": ENV.upper() } )

with open(S3_BASELINE_OBJECT_NAME, "w") as f:
    # Print CSV header
    print("fully_qualified_table_name,database_name,table_name,has_partitions,s3_location,partition_count,fingerprint,timestamp", file=f)

    print(f"Getting S3 locations for tables with filter database='{FILTER_DATABASE}' and filter tables='{FILTER_TABLES}'")
                
    s3_locations = get_s3_locations_for_tables(FILTER_DATABASE, FILTER_TABLES)

    # Iterate through Hive tables
    for s3_location in s3_locations:
        
        info = get_partition_info(s3, f"{s3_location.location}")
        print(f"{s3_location.fully_qualified_table_name},{s3_location.database_name},{s3_location.table_name},{s3_location.has_partitions},{info['s3_location']},{info['partition_count']},{info['fingerprint']},{info['timestamp']}", file=f)

# upload the file to S3 to make it available
if S3_UPLOAD_ENABLED:
    logger.info(f"Uploading {S3_BASELINE_OBJECT_NAME} to s3://{S3_ADMIN_BUCKET}/{S3_BASELINE_OBJECT_NAME}")

    s3.upload_file(S3_BASELINE_OBJECT_NAME, S3_ADMIN_BUCKET, S3_BASELINE_OBJECT_NAME)