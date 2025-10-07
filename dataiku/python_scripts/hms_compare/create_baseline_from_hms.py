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
from datetime import datetime
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse
from sqlalchemy import create_engine, select, text, Column, Integer, String, DateTime, inspect
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZONE = get_zone_name(upper=True)

# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=True)
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
HMS_BASELINE_OBJECT_NAME = get_param('HMS_BASELINE_OBJECT_NAME', 'baseline_s3.csv')

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

def get_table_names(filter_tables=None):
    if src_engine.dialect.name == 'postgresql':
        catalog_name = ""
    else:
        catalog_name = f"{HMS_TRINO_CATALOG}."

    if filter_tables:
        filter_tables_str = ",".join([f"'{tbl.strip()}'" for tbl in filter_tables.split(",")])
        filter_where_clause = f"AND table_name IN ({filter_tables_str})"
    else:
        filter_where_clause = ""

    with src_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT table_name 
            FROM {catalog_name}information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            {filter_where_clause}
        """))
        return [row[0] for row in result]

def quote_ident(name: str, dialect):
    return dialect.identifier_preparer.quote(name)


def generate_baseline_for_table(engine, table: str, schema: str = "public"):
    """
    """

    if src_engine.dialect.name == 'postgresql':
        catalog_name = ""
    else:
        catalog_name = f"{HMS_TRINO_CATALOG}."

    with src_engine.connect() as conn:
        # Step 1: Get primary key columns
        inspector = inspect(conn)
        pk_columns = inspector.get_pk_constraint(table, schema=schema)['constrained_columns']

        if not pk_columns:
            print(f"No primary key found for table {schema}.{table}")
        else:
            # Step 2: Quote identifiers for safety
            full_table = f"{schema}.{quote_ident(table,dialect=conn.dialect)}"
            order_by_clause = ", ".join("t." + quote_ident(col,dialect=conn.dialect) for col in pk_columns)

            print (f"Generating fingerprint for table {full_table} using PK columns: {pk_columns}")

            create_time_table_join = ""
            create_time_col = ""
            create_time_col_alias = ""
            if "PART_ID" in pk_columns:
                create_time_table_join = f"LEFT JOIN {catalog_name}public.\"PARTITIONS\" ct ON ct.\"PART_ID\" = t.\"PART_ID\""
                create_time_col = f', ct."CREATE_TIME" AS create_time'
                create_time_col_alias = f', MAX(create_time) AS max_create_time'
            if "TBL_ID" in pk_columns:
                create_time_table_join = f"LEFT JOIN {catalog_name}public.\"TBLS\" ct ON ct.\"TBL_ID\" = t.\"TBL_ID\""
                create_time_col = f', ct."CREATE_TIME" AS create_time'
                create_time_col_alias = f', MAX(create_time) AS max_create_time'
            if "DB_ID" in pk_columns:
                create_time_table_join = f"LEFT JOIN {catalog_name}public.\"DBS\" ct ON ct.\"DB_ID\" = t.\"DB_ID\""
                create_time_col = f', ct."CREATE_TIME" AS create_time'
                create_time_col_alias = f', MAX(create_time) AS max_create_time'

            # Step 3: Prepare and execute SQL
            query = text(f"""
                SELECT COUNT(*) AS row_count
                , md5(string_agg(md5(row_text), '')) AS fingerprint
                {create_time_col_alias}
                FROM (
                    SELECT row(t.*)::text AS row_text
                    {create_time_col}
                    FROM {full_table} t
                    {create_time_table_join}
                    ORDER BY {order_by_clause}
                ) AS subquery  
            """)

        logger.debug(f"Executing SQL: {stmt}")
        
        s3_locations = session.execute(stmt).scalars().all()
        return s3_locations

HMS_BASELINE_OBJECT_NAME = replace_vars_in_string(HMS_BASELINE_OBJECT_NAME, { "zone": ZONE.upper(), "env": ENV.upper() } )

# Dynamically get table names from the source DB
tables = get_table_names()

def create_baseline():
    tables = get_table_names(FILTER_TABLES)

    S3_BASELINE_OBJECT_NAME = "hms_baseline.csv"
    with open(S3_BASELINE_OBJECT_NAME, "w") as f:
        # Print CSV header
        print("table_name,count,fingerprint,timestamp", file=f)

        for table in tables:
            if (table == 'COMPACTION_METRICS_CACHE' or table == 'WRITE_SET' or table == 'TXN_TO_WRITE_ID' or table == 'NEXT_WRITE_ID' 
                or table == 'MIN_HISTORY_WRITE_ID' or table == 'TXN_COMPONENTS' or table == 'COMPLETED_TXN_COMPONENTS' or table == 'TXN_LOCK_TBL'
                or table == 'NEXT_LOCK_ID' or table == 'NEXT_COMPACTION_QUEUE_ID' ):
                continue
            else:
                with src_engine.connect() as conn:
                    baseline = generate_baseline_for_table(src_engine, table, "public")

                if baseline is not None:
                    count = baseline[0]
                    if baseline[1] is not None:
                        fingerprint = baseline[1]
                    else:
                        fingerprint = ''
                    timestamp = baseline[2] if len(baseline) > 2 else 0
                    print(f"{table},{count},{fingerprint},{timestamp}", file=f)
                    
        # upload the file to S3 to make it available
        if S3_UPLOAD_ENABLED:
            logger.info(f"Uploading {HMS_BASELINE_OBJECT_NAME} to s3://{S3_ADMIN_BUCKET}/{HMS_BASELINE_OBJECT_NAME}")

            s3.upload_file(HMS_BASELINE_OBJECT_NAME, S3_ADMIN_BUCKET, HMS_BASELINE_OBJECT_NAME)

create_baseline()
