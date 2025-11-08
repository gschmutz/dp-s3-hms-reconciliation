"""
This module provides functionality for comparing table metrics between a baseline (read from Kafka) and actual counts (queried from Trino).
It is designed to be used in a Dataiku scenario, but can also run outside of Dataiku with environment variables.
Main Features:
--------------
- Reads configuration and credentials from Dataiku scenario variables or environment variables.
- Connects to Trino using SQLAlchemy for querying actual table counts.
- Consumes Kafka messages to extract baseline metrics for tables, supporting SSL and SASL_SSL security protocols.
- Filters Kafka messages by catalog, schema, and table name if specified.
- Stores the latest metric for each table based on event timestamp.
- Provides pytest-based tests to validate Trino connectivity and compare baseline vs. actual counts for each table.
Functions:
----------
- get_param(name, default=None): Retrieves parameter values from scenario variables or environment variables.
- get_credential(name, default=None): Retrieves credentials from Dataiku client secrets or returns default.
- get_baseline(table): Returns the latest baseline metric for a given table.
- get_actual_count(table, timestamp_column=None, baseline_timestamp=None): Queries Trino for the actual row count, optionally filtered by timestamp.
- init_actual_values_from_kafka(filter_catalog=None, filter_schema=None, filter_tables=None): Consumes Kafka topic and returns latest metrics for tables, applying optional filters.
Tests:
------
- test_trino_availability(): Checks Trino connection availability.
- test_value_compare(fully_qualified_table_name): Parameterized test comparing baseline and actual counts for each table.
Environment Variables / Scenario Variables:
-------------------------------------------
- FILTER_CATALOG, FILTER_SCHEMA, FILTER_TABLE: Optional filters for Kafka consumption.
- TRINO_USER, TRINO_PASSWORD, TRINO_HOST, TRINO_PORT, TRINO_CATALOG: Trino connection details.
- KAFKA_BOOTSTRAP_SERVERS, KAFKA_SECURITY_PROTOCOL, KAFKA_SSL_CA_LOCATION, KAFKA_SSL_CERT_LOCATION, KAFKA_SSL_KEY_LOCATION, KAFKA_SSL_KEY_PASSWORD, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, KAFKA_TOPIC_NAME: Kafka connection details.
Usage:
------
- Configure environment/scenario variables as needed.
- Run the module with pytest to execute the tests and validate table metric reconciliation.

TODO: run in batches
"""
import hashlib
import pytest
import os
import io
import sys
import time
import json
import logging
import boto3
import pandas as pd

from sqlalchemy import create_engine,text,inspect
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.serialization import SerializationContext, MessageField, StringDeserializer, Deserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer   
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, get_s3_location_list, replace_vars_in_string

from typing import Optional

 
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
ZONE = get_zone_name(upper=True)
# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
ENV = get_param('ENV', 'UAT', upper=True)

FILTER_CATALOGS = get_param('FILTER_CATALOGS', "")
FILTER_SCHEMA = get_param('FILTER_SCHEMA', "")
FILTER_TABLES = get_param('FILTER_TABLES', "")
FILTER_BATCH = get_param('FILTER_BATCH', "")                    # either all or a specific batch number, if empty it will not use the batch filter at all
FILTER_STAGE = get_param('FILTER_STAGE', "")                    # either all or a specific stage number, if empty it will not use the stage filter at all

RESTORE_TARGET_TIMESTAMP_MS = get_param('RESTORE_TARGET_TIMESTAMP_MS', None)  # if set will be used to restore the baseline counts from Kafka at a specific point in time (unix timestamp in milliseconds)
 
TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

HMS_TRINO_CATALOG = get_param('HMS_TRINO_CATALOG', 'minio')

KAFKA_BOOTSTRAP_SERVERS = get_param('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_SECURITY_PROTOCOL = get_param('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
KAFKA_SSL_CA_LOCATION = get_param('KAFKA_SSL_CA_LOCATION', '/path/to/ca.pem')
KAFKA_SSL_CERT_LOCATION = get_param('KAFKA_SSL_CERT_LOCATION', '/path/to/client_cert.pem')
KAFKA_SSL_KEY_LOCATION = get_param('KAFKA_SSL_KEY_LOCATION', '/path/to/client_key.pem')
KAFKA_SSL_KEY_PASSWORD = get_credential('KAFKA_SSL_KEY_PASSWORD', '<PASSWORD_NOT_SET>')  # if your key is password protected
KAFKA_SASL_USERNAME = get_credential('KAFKA_SASL_USERNAME', '<USERNAME_NOT_SET>')
KAFKA_SASL_PASSWORD = get_credential('KAFKA_SASL_PASSWORD', '<PASSWORD_NOT_SET>')
KAFKA_TOPIC_NAME = get_param('KAFKA_TOPIC_NAME', 'dpraw_execution_status_log_v1')

SCHEMA_REGISTRY_URL =  get_param('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
SCHEMA_REGISTRY_SSL_CA_LOCATION = get_param('SCHEMA_REGISTRY_SSL_CA_LOCATION', '/path/to/ca.pem')
 
# Connect to MinIO or AWS S3
ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')
S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_ADMIN_BUCKET = replace_vars_in_string(S3_ADMIN_BUCKET, { "zone": ZONE.upper(), "env": ENV.upper() } )

S3_LOCATION_LIST_OBJECT_NAME = get_param('S3_LOCATION_LIST_OBJECT_NAME', 's3_locations.csv')
S3_LOCATION_LIST_OBJECT_NAME = replace_vars_in_string(S3_LOCATION_LIST_OBJECT_NAME, { "database": FILTER_SCHEMA.upper(), "schema": FILTER_SCHEMA.upper(), "zone": ZONE.upper(), "env": ENV.upper() } )

# Construct connection URLs
trino_url = f'trino://{TRINO_USER}:{TRINO_PASSWORD}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}'
if TRINO_USE_SSL:
    trino_url = f'{trino_url}?protocol=https&verify=false'

# Setup connections
trino_engine = create_engine(trino_url)

# Create a session and S3 client
s3 = boto3.client('s3')

# Create S3 client configuration
s3_config = {"service_name": "s3"}
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', f'admin')
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', f'abc123abc123')

if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    s3_config["aws_access_key_id"] = AWS_ACCESS_KEY
    s3_config["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
if ENDPOINT_URL:
    s3_config["endpoint_url"] = ENDPOINT_URL
    s3_config["verify"] = False  # Disable SSL verification for self-signed certificates

s3 = boto3.client(**s3_config)
 
# Schema Registry
schema_registry_configuration_confluent = {
    'url': SCHEMA_REGISTRY_URL,
    'ssl.ca.location': SCHEMA_REGISTRY_SSL_CA_LOCATION
}

# Kafka Consumer config
consumer_conf_ssl = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'avro-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'enable.partition.eof': True,
    'security.protocol': 'SSL',
    'ssl.ca.location': KAFKA_SSL_CA_LOCATION,
    'ssl.certificate.location': KAFKA_SSL_CERT_LOCATION,
    'ssl.key.location': KAFKA_SSL_KEY_LOCATION,
    'ssl.key.password': KAFKA_SSL_KEY_PASSWORD    
}
 
consumer_conf_sasl_ssl = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 's_DPR-ESP-UAT_cg_tmc',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'enable.partition.eof': True,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': KAFKA_SASL_USERNAME,
    'sasl.password': KAFKA_SASL_PASSWORD,
    'ssl.ca.location': KAFKA_SSL_CA_LOCATION
}
 
consumer_conf_plaintext = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'avro-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'enable.partition.eof': True,
}
 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_baseline(table):
    return latest_values[table]

def is_column_name_part_of_table(column_name: str, table: str) -> bool:
    """
    Check if a given column name exists in the specified table using SQLAlchemy's inspector.
 
    Args:
        column_name (str): The name of the column to check.
        table (str): The name of the table to inspect (as catalog.schema.table_name).
 
    Returns:
        bool: True if the column exists in the table, False otherwise.
    """
    with trino_engine.connect() as conn:
        inspector = inspect(conn)
        # Split table into catalog, schema, and table_name
        parts = table.split('.')
        if len(parts) == 3:
            catalog, schema, table_name = parts
        elif len(parts) == 2:
            schema, table_name = parts
            catalog = None
        else:
            table_name = parts[0]
            schema = None
            catalog = None
        columns = [col['name'] for col in inspector.get_columns(table_name, schema)]
        return column_name in columns

def create_fingerprint(partitions: str) -> str:
    """
    Create a fingerprint string from a comma-separated list of partition names.
    
    Args:
        partitions (str): Comma-separated list of partition names.
        
    Returns:
        str: A fingerprint string representing the partitions.
    """
    partition_fingerprint = ""
    if partitions:
        partition_fingerprint = hashlib.sha256(
            partitions.encode('utf-8')
        ).hexdigest()
    return partition_fingerprint

def get_actual_row_count(table: str, timestamp_column: Optional[str] = None, baseline_timestamp: Optional[int] = None) -> int:
    """
    Retrieve the count of rows from a specified table over Trino, optionally filtered by a timestamp column and baseline timestamp.
    Please define the various environment variables for the connection to Trino including the catalog and schema.
 
    Args:
 
        table (str): The name of the table to query (as catalog.schema.table_name).
        timestamp_column (str, optional): The name of the timestamp column to filter by. If provided, the count will be filtered where the timestamp column is less than or equal to the baseline timestamp.
        baseline_timestamp (int or float, optional): The baseline timestamp (in Unix time) to use for filtering rows.
 
    Returns:
        int: The count of rows in the table, optionally filtered by the timestamp column and baseline timestamp.
 
    Raises:
        Exception: If there is an error executing the query.
    """
 
    if not timestamp_column:
        # Check if the table has the specified column using SQLAlchemy's inspector

        if is_column_name_part_of_table("dp_load_timestamp", table):
            timestamp_column = "dp_load_timestamp"

    with trino_engine.connect() as conn:
 
        if not timestamp_column:
            query = text(
                f'''
                SELECT
                    COUNT(*) AS count
                FROM {table}
                '''
            )
        else:
            query = text(
                f'''
                SELECT
                    COUNT(*) AS count
                FROM {table}
                WHERE {timestamp_column} <= from_unixtime({baseline_timestamp} / 1000 + 1)
                '''
            )
        
        logger.debug(f"Executing query: {query}")
        try:
            count = conn.execute(query).scalar()
        except Exception as e:
            logger.error(f"Error executing query for table '{table}': {str(e)}")
            count = None
    return count

def get_actual_partition_metrics(table_name: str, database_name: str, hmsdb_catalog_name: str, baseline_timestamp: Optional[int] = None) -> tuple[int, str]:
    """
    Returns partition count and partition list for a given table.
    
    Returns:
        tuple[int, str]: A tuple containing (partition_count, partition_list)
    """

    with trino_engine.connect() as conn:
        query = text(
            f' SELECT d."NAME" as database_name,'
            f'   t."TBL_NAME" as table_name,'
            f'   t."TBL_TYPE" as table_type,'
            f"   CASE"
            f"      WHEN COALESCE(pk.has_partitions, 0) >= 1 then 'Y'"
            f"      ELSE 'N'"
            f"   END as has_partitions,"
            f'   COUNT(p."PART_ID") as partition_count,'
            f'   ARRAY_JOIN(ARRAY_AGG(p."PART_NAME" ORDER BY p."PART_ID"), \',\') as partition_list'
            f' FROM {hmsdb_catalog_name}.public."TBLS" t'
            f' JOIN {hmsdb_catalog_name}.public."DBS" d'
            f'   ON t."DB_ID" = d."DB_ID"'
            f" LEFT JOIN ("
            f'   SELECT pk."TBL_ID",'
            f"      GREATEST(SIGN(COUNT(*)), 0) as has_partitions"
            f'   FROM {hmsdb_catalog_name}.public."PARTITION_KEYS" pk'
            f'   GROUP BY pk."TBL_ID"'
            f' ) pk ON t."TBL_ID" = pk."TBL_ID"'
            f' LEFT JOIN {hmsdb_catalog_name}.public."PARTITIONS" p '
            f'   ON t."TBL_ID" = p."TBL_ID"'
            f" WHERE LOWER(t.\"TBL_NAME\") = LOWER('{table_name}')"
            f" AND LOWER(d.\"NAME\") = LOWER('{database_name}')"
            f' GROUP BY d."NAME",'
            f'   t."TBL_NAME",'
            f'   t."TBL_TYPE",'
            f"   pk.has_partitions"
        )

        logger.debug(f"Executing query: {query}")
        try:
            result = conn.execute(query).fetchone()
            if result:
                partition_count = result[4]  # partition_count column
                partition_list = result[5] or ""  # partition_list column, default to empty string if None
                partition_fingerprint = create_fingerprint(partition_list)
                return partition_count, partition_fingerprint
            else:
                return -1, ""
        except Exception as e:
            logger.error(f"Error executing query for table '{table_name}': {str(e)}")
            return -1, ""
        
def init_actual_values_from_kafka(filter_catalogs: Optional[str] = None, filter_schema: Optional[str] = None, filter_tables: Optional[str] = None, filter_batch: Optional[str] = None, filter_stage: Optional[str] = None) -> dict:
    """
        Initializes and returns the latest actual table metric values by consuming messages from a Kafka topic.
        This function connects to a Kafka topic, reads messages containing table metric information,
        and filters the results based on the provided catalog, schema, table names, batch, and stage filters.
        Only metrics from successful 'post_create_table_metric' jobs are considered, and only the latest metric
        per table (based on event timestamp) is retained. Tables are further filtered to ensure they exist in
        the provided S3 location list.
        Args:
            filter_catalogs (Optional[str]): Catalog name(s) to filter metrics. If None, no catalog filtering is applied.
            filter_schema (Optional[str]): Schema name to filter metrics. If None, no schema filtering is applied.
            filter_tables (Optional[str]): Table name(s) to filter metrics. If None, no table filtering is applied.
            filter_batch (Optional[str]): Batch identifier to filter S3 locations. If None, no batch filtering is applied.
            filter_stage (Optional[str]): Stage identifier to filter metrics. If None, no stage filtering is applied.
        Returns:
            dict: A dictionary where keys are fully qualified table names (schema.table_name) and values are dictionaries
                  containing the latest metric values for each table, including timestamp, timestamp_column, count, and table_name.
        Raises:
            Exception: If an error occurs during Kafka consumption or message processing.
        Notes:
            - Only metrics from tables present in the S3 location list are included.
            - Only the latest metric per table (based on event_time) is retained.
            - The function logs progress and errors using the configured logger.    
    """
    latest_values: dict[str, dict] = {}
    logger.info(f"running init_actual_values_from_kafka (filter_catalogs={filter_catalogs}, filter_schema={filter_schema}, filter_tables={filter_tables})")
 
    # the list of S3 locations with the tables (potentially filtered by batch)
    s3_location_list = get_s3_location_list(s3, S3_ADMIN_BUCKET, S3_LOCATION_LIST_OBJECT_NAME, filter_batch, filter_stage)

    consume_until_timestamp_ms: int = int(time.time()) * 1000
    if RESTORE_TARGET_TIMESTAMP_MS:
        consume_until_timestamp_ms = int(RESTORE_TARGET_TIMESTAMP_MS)

    # read from kafka
    if KAFKA_SECURITY_PROTOCOL == 'SSL':
        consumer = Consumer(consumer_conf_ssl)
    elif KAFKA_SECURITY_PROTOCOL == 'SASL_SSL':
        consumer = Consumer(consumer_conf_sasl_ssl)
    else:
        consumer = Consumer(consumer_conf_plaintext)
 
    schema_registry = SchemaRegistryClient(schema_registry_configuration_confluent)
    string_deserializer = StringDeserializer('utf_8')
    avro_deserializer = AvroDeserializer(schema_registry)

    topic = KAFKA_TOPIC_NAME
 
    # Fetch metadata to get partition info
    metadata = consumer.list_topics(topic, timeout=10)
 
    logger.info(f"metadata: {metadata.topics}")
 
    partitions = metadata.topics[topic].partitions
    topic_partitions = [TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions]
 
    consumer.assign(topic_partitions)
    #consumer.subscribe([topic])
    logger.info(f"assigned the following partitions: {partitions}")
 
    time.sleep(2)  # wait for messages to be available
 
    try:
        logger.info(f"Consuming messages from topic: {topic}")
        eof_count = 0
   
        while True:
            msg = consumer.poll(timeout = 2.0)
 
            if msg is None:
                # No message in this poll
                logger.info("no messages ....")
                continue
           
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    eof_count += 1
                    # Optional: stop when all partitions have been consumed
 
                    logger.info(f"EOF count: {eof_count}, Total partitions: {len(topic_partitions)}")
 
                    if eof_count == len(topic_partitions):
                        logger.info("All partitions consumed.")
                        break
                else:
                    logger.info("ERROR")                    
                    logger.info("Consumer error: {}".format(msg.error()))
                    continue
            else:
                eof_count = 0
                # deserialize
                data:dict = avro_deserializer(msg.value(), None)
 
                # find the post_create_table_metric job
                if data["steps"] and "post_create_table_metric" in data["steps"]:
                    post_create_table_metric_step: dict = data["steps"]["post_create_table_metric"]

                    if "job_outcome" in post_create_table_metric_step and post_create_table_metric_step["job_outcome"] == "SUCCESS":                   
                        metric: dict = json.loads(post_create_table_metric_step["job_exit_message"])
 
                        object = f"{metric.get('catalog')}.{metric.get('schema')}.{metric.get('table_name')}"

                        # apply filters it set
                        # Convert comma-separated filter_catalogs string to list if necessary
                        if filter_catalogs:
                            filter_catalogs_list = [c.strip() for c in filter_catalogs.split(",") if c.strip()]
                            if metric.get('catalog') not in filter_catalogs_list:
                                logger.info(f"Skipping {object} as catalog {metric.get('catalog')} is not in the filter_catalogs list")
                                continue
                        if filter_schema and metric.get('schema') != filter_schema:
                                logger.info(f"Skipping {object} as schema {metric.get('schema')} is not in the filter_schema list")
                                continue
                        # Convert comma-separated filter_tables string to list if necessary
                        if filter_tables:
                            filter_tables_list = [t.strip() for t in filter_tables.split(",") if t.strip()]
                            if metric.get('table_name') not in filter_tables_list:
                                logger.info(f"Skipping {object} as table {metric.get('table_name')} is not in the filter_tables list")
                                continue

                        # check if the table is in the list of S3 locations and if not skip it
                        fully_qualified_table_name = f"{metric.get('schema')}.{metric.get('table_name')}"
                        if fully_qualified_table_name not in s3_location_list["fully_qualified_table_name"].values:
                            logger.info(f"Skipping {object} as it is not in the S3 location list")
                            continue

                        timestamp = metric.get('event_time', 0)  # Assuming event_time is in milliseconds
                        if timestamp > consume_until_timestamp_ms:
                            logger.info(f"Skipping {object} as event_time={timestamp} is after the consume_until_timestamp_ms={consume_until_timestamp_ms}")
                            continue
    
                        # Store only the latest value based on timestamp
                        key = fully_qualified_table_name
                        if key:
                            if key not in latest_values or timestamp > latest_values[key]['timestamp']:
                                latest_values[key] = {
                                    'timestamp': timestamp,
                                    'timestamp_column': metric.get('timestamp_column', None),
                                    'count': metric.get('count', 0),
                                    'table_name': metric.get('table_name', None),
                                    'database_name': metric.get('schema', None)
                                }
                                logger.debug(f"Updated latest value for {key}: {latest_values[key]}")
                            else:
                                logger.debug(f"Skipped older message for {key}: timestamp {timestamp} <= {latest_values[key]['timestamp']}")
 
    except KeyboardInterrupt:
        logger.info("Stopping consumer.")
    except Exception as ex:
        logger.error(f"Error {ex}")
    finally:
        consumer.close()
        for table, values in latest_values.items():
            logger.info(f"Table: {table}, Values: {values['timestamp']}, {values['count']}, {values['timestamp_column']}")
        #logger.info(f"init_actual_values_from_kafka() completed. Found {len(latest_values)} tables: {list(latest_values.keys())}")
    return latest_values

# all the latest values from Kafka (applying a potential filer set via environment variables)
latest_values = init_actual_values_from_kafka(FILTER_CATALOGS, FILTER_SCHEMA, FILTER_TABLES, FILTER_BATCH, FILTER_STAGE)
 
# collection of fully qualified table names
fully_qualified_table_names = list(latest_values.keys())
 
def test_trino_availability():
    with trino_engine.connect() as conn:
        try:
            result = conn.execute(text("SELECT 1"))
            logger.info(f"Trino is available: {result.fetchall()}")
            assert True
        except Exception as e:
            logger.error(f"Trino connection failed: {str(e)}")
            assert False
 
@pytest.mark.parametrize("fully_qualified_table_name", fully_qualified_table_names)
def test_row_count_compare(fully_qualified_table_name):
    """
    Test function to compare the row counts between baseline and actual data for a given table.
 
    This test is parameterized to run for each table in the `tables` list. For each table:
    - Retrieves the baseline row count, timestamp column, and event time using `get_baseline`.
    - Retrieves the actual row count from the target data source using `get_actual_row_count`, filtered by the timestamp column and baseline timestamp.
    - Asserts that the baseline and actual counts are equal, raising an assertion error with a descriptive message if they do not match.
 
    Args:
        table (str): The name of the table to compare.
 
    Raises:
        AssertionError: If the baseline count does not match the actual row count for the given table.
    """
    baseline_row_count = get_baseline(fully_qualified_table_name)['row_count']
    timestamp_column = get_baseline(fully_qualified_table_name)['timestamp_column']
    event_time = get_baseline(fully_qualified_table_name)['timestamp']
    actual_row_count = get_actual_row_count(fully_qualified_table_name, timestamp_column=timestamp_column, baseline_timestamp=event_time)
 
    assert baseline_row_count == actual_row_count, f"Mismatch in table '{fully_qualified_table_name}': Baseline Row Count from last job processing ({baseline_row_count}) at timestamp ({timestamp_column}={event_time}) does not match actual count retrieved from Trino ({actual_row_count})"

@pytest.mark.parametrize("fully_qualified_table_name", fully_qualified_table_names)
def test_partition_count_compare(fully_qualified_table_name):
    """
    Test function to compare the partition counts between baseline and actual data for a given table.
 
    This test is parameterized to run for each table in the `tables` list. For each table:
    - Retrieves the baseline partition count, timestamp column, and event time using `get_baseline`.
    - Retrieves the actual partition count from the target data source using `get_actual_partition_metrics`, filtered by the timestamp column and baseline timestamp.
    - Asserts that the baseline and actual partition counts are equal, raising an assertion error with a descriptive message if they do not match.
 
    Args:
        table (str): The name of the table to compare.
 
    Raises:
        AssertionError: If the baseline count does not match the actual partition count for the given table.
    """
    table_name = get_baseline(fully_qualified_table_name)['table_name']
    database_name = get_baseline(fully_qualified_table_name)['database_name']

    baseline_partition_count = get_baseline(fully_qualified_table_name)['partition_count']
    timestamp_column = get_baseline(fully_qualified_table_name)['timestamp_column']
    event_time = get_baseline(fully_qualified_table_name)['timestamp']
    
    actual_partition_count, actual_partition_fingerprint = get_actual_partition_metrics(table_name=table_name, database_name=database_name, hmsdb_catalog_name="postgres", baseline_timestamp=event_time)

    assert baseline_partition_count == actual_partition_count, f"Mismatch in table '{fully_qualified_table_name}': Baseline Partition Count from last job processing ({baseline_partition_count}) at timestamp ({timestamp_column}={event_time}) does not match actual count retrieved from Trino ({actual_partition_count})"
