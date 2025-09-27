"""
This script generates a list of S3 locations for tables in a Hive Metastore, batches them according to a specified strategy, and uploads the resulting CSV file to an S3-compatible storage (MinIO or AWS S3). It supports running inside Dataiku DSS scenarios or as a standalone script, retrieving configuration and credentials from scenario variables or environment variables.
Main functionalities:
- Retrieves configuration parameters and credentials from Dataiku scenario variables or environment variables.
- Connects to the Hive Metastore database via PostgreSQL or Trino.
- Queries metadata tables to extract table information, including S3 locations, partitioning, and creation times.
- Supports batching strategies for dividing tables into groups: balanced by partition size, by table prefix, or by creation time.
- Writes the extracted and batched table metadata to a CSV file.
- Optionally uploads the CSV file to an S3 bucket.
Key components:
- `get_param`: Retrieves configuration parameters.
- `get_credential`: Retrieves secret credentials.
- `get_s3_locations_with_batches`: Queries the metastore and returns a list of S3Location objects, batched according to the selected strategy.
- `replace_vars_in_string`: Utility for variable substitution in strings.
- S3 upload logic for making the CSV file available in object storage.
Environment variables and scenario parameters control filtering, batching, database access, and S3 upload settings.

"""
import select
import boto3
import re
import os
import sys
import logging
import pandas as pd
from datetime import datetime
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse
from requests import Session, session
from sqlalchemy import create_engine, select, text, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, replace_vars_in_string

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZONE = get_zone_name(upper=True)
# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=True)

FILTER_DATABASE = get_param('FILTER_DATABASE', "")
FILTER_TABLES = get_param('FILTER_TABLES', "")

BATCHING_STRATEGY = get_param('BATCHING_STRATEGY', 'balanced_by_partition_size')      # balanced_by_partition_size | by_table_prefix | create_time
NUMBER_OF_BATCHES = get_param('NUMBER_OF_BATCHES', "3")
NUMBER_OF_STAGES = get_param('NUMBER_OF_STAGES', "1") 

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
S3_LOCATION_LIST_OBJECT_NAME = get_param('S3_LOCATION_LIST_OBJECT_NAME', 's3_locations.csv')
S3_LOCATION_LIST_OBJECT_NAME = replace_vars_in_string(S3_LOCATION_LIST_OBJECT_NAME, { "database": FILTER_DATABASE.upper(), "zone": ZONE.upper(), "env": ENV.upper() } )

# Setup connections to the metadatastore, either directly to postgresql or via trino
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
    s3_config["verify"] = False     # Disable SSL verification for self-signed certificates
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
    last_create_time = Column(Integer, nullable=False)
    row_num = Column(Integer, nullable=False)
    batch = Column(Integer, nullable=False)
    stage = Column(Integer, nullable=False)

    def __repr__(self):
        return f"<S3Location(fully_qualified_table_name={self.fully_qualified_table_name}, database_name={self.database_name}, table_name='{self.table_name}', table_type='{self.table_type}', location='{self.location}', has_partitions='{self.has_partitions}', partition_count={self.partition_count}, row_num={self.row_num}, batch={self.batch}, stage={self.stage})>"

def get_s3_locations_with_batches(batching_strategy: str="", number_of_batches: str="", filter_database: str="", filter_tables: str="") -> list[S3Location]:
    """
    Retrieves a list of S3Location objects from the database, applying batching and filtering strategies.

    Args:
        batching_strategy (str, optional): The strategy used to batch tables. Supported values are 
            'balanced_by_partition_size', 'by_table_prefix', and 'create_time'.
        number_of_batches (str, optional): The number of batches to divide the tables into. Defaults to 1 if not provided.
        filter_database (str, optional): The name of the database to filter tables by.
        filter_tables (str, optional): A comma-separated list of table names to filter by.

    Returns:
        list[S3Location]: A list of S3Location objects, each annotated with batch and stage information.

    Raises:
        ValueError: If an unknown batching strategy is provided.

    Notes:
        - The batching expression and filtering clause are dynamically constructed based on the input parameters.
        - The function supports both PostgreSQL and Trino dialects for catalog naming.
        - The query ranks and groups tables by partition count, table prefix, and creation time for batching.
    """

    nof_batches = number_of_batches if number_of_batches else 1
    if batching_strategy not in ['balanced_by_partition_size', 'by_table_prefix', 'create_time']:    
        raise ValueError(f"Unknown batch strategy: {batching_strategy}")
    elif batching_strategy == 'balanced_by_partition_size':
        batching_expr = f"(ranked_by_partition_count % {number_of_batches})"
    elif batching_strategy == 'by_table_prefix' and number_of_batches: 
        batching_expr = f"(group_nr_by_prefix % {number_of_batches})"
    elif batching_strategy == 'by_table_prefix' and not number_of_batches: 
        batching_expr = "group_nr_by_prefix"
    elif batching_strategy == 'create_time' and number_of_batches: 
        batching_expr = "batched_by_last_create_time"

    nof_stages = NUMBER_OF_STAGES if NUMBER_OF_STAGES else 1

    if src_engine.dialect.name == 'postgresql':
        catalog_name = ""
    else:
        catalog_name = f"{HMS_TRINO_CATALOG}."

    if filter_database and filter_tables:
        filter_where_clause = f"WHERE d.\"NAME\" = '{filter_database}' AND t.\"TBL_NAME\" IN ({filter_tables})"
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

        stmt = text(f"""
                SELECT
                    r.*,
                    {batching_expr} + 1 AS batch,
                    FLOOR ({batching_expr} / ({number_of_batches}/{nof_stages})) + 1 AS stage
                FROM
                    (
                    SELECT
                        r.*,
                        row_number() over (
                            order by r.partition_count desc) as ranked_by_partition_count,
                        dense_rank() over (
                            order by r.prefix) as group_nr_by_prefix,
                        NTILE({nof_batches}) over (
                            order by r.last_create_time desc) as batched_by_last_create_time
                    FROM
                        (
                        SELECT
                            CONCAT(d."NAME", '.', t."TBL_NAME") as fully_qualified_table_name,
                            d."NAME" as database_name,
                            t."TBL_NAME" as table_name,
                            split_part(t."TBL_NAME", '_', 1) as prefix,
                            t."TBL_TYPE" as table_type,
                            s."LOCATION" as location,
                            case
                                when pk.has_partitions >= 1 then 'Y'
                                else 'N'
                            end as has_partitions,
                            COUNT(p."PART_ID") as partition_count,
                            GREATEST(MAX(t."CREATE_TIME"), MAX(p."CREATE_TIME")) AS last_create_time
                        FROM
                            {catalog_name}public."TBLS" t
                        JOIN {catalog_name}public."DBS" d on
                            t."DB_ID" = d."DB_ID"
                        JOIN {catalog_name}public."SDS" s on
                            t."SD_ID" = s."SD_ID"
                        LEFT JOIN (
                            SELECT
                                pk."TBL_ID",
                                greatest(SIGN(COUNT(*)), 0) as has_partitions
                            FROM
                                {catalog_name}public."PARTITION_KEYS" pk
                            GROUP BY
                                pk."TBL_ID") pk on
                            t."TBL_ID" = pk."TBL_ID"
                        left JOIN {catalog_name}public."PARTITIONS" p on
                            t."TBL_ID" = p."TBL_ID"
                        {filter_where_clause}    
                        GROUP BY
                            d."NAME",
                            t."TBL_NAME",
                            t."TBL_TYPE",
                            s."LOCATION",
                            pk.has_partitions
                    ) r	
                ) r
                ORDER BY {batching_expr}, prefix
            """)
            
        logger.debug("Executing query to retrieve S3 locations with batching: {stmt}")
        s3_locations = session.execute(select(S3Location).from_statement(stmt)).scalars().all()

        return s3_locations

with open(S3_LOCATION_LIST_OBJECT_NAME, "w") as f:
    # Print CSV header
    print("fully_qualified_table_name,database_name,table_name,table_type,s3_location,has_partitions,partition_count,last_create_time,batch,stage", file=f)
                
    # Iterate through Hive tables
    s3_locations = get_s3_locations_with_batches(BATCHING_STRATEGY, NUMBER_OF_BATCHES, FILTER_DATABASE, FILTER_TABLES)
    print(f"Found {len(s3_locations)} S3 locations in Hive Metastore")
    for s3_location in s3_locations:
        print(f"{s3_location.fully_qualified_table_name},{s3_location.database_name},{s3_location.table_name},{s3_location.table_type},{s3_location.location},{s3_location.has_partitions},{s3_location.partition_count},{s3_location.last_create_time},{s3_location.batch},{s3_location.stage}", file=f)

# upload the file to S3 to make it available
if S3_UPLOAD_ENABLED:
    logger.info(f"Uploading {S3_LOCATION_LIST_OBJECT_NAME} to s3://{S3_ADMIN_BUCKET}/{S3_LOCATION_LIST_OBJECT_NAME}")
    
    s3.upload_file(S3_LOCATION_LIST_OBJECT_NAME, S3_ADMIN_BUCKET, S3_LOCATION_LIST_OBJECT_NAME)

# create summary
df = pd.read_csv(S3_LOCATION_LIST_OBJECT_NAME)
summary = (
    df.groupby("stage")
      .agg(
          batches=("batch", lambda x: list(dict.fromkeys(x))),  # list of unique values, order preserved
          tables_count=("batch", "size"),       # total number of rows
      )
      .reset_index()
)
logger.info(f"Summary of S3 locations by stage")
logger.info("\n%s", summary)