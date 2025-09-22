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
- init_actual_values_from_kafka(filter_catalog=None, filter_schema=None, filter_table=None): Consumes Kafka topic and returns latest metrics for tables, applying optional filters.
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
import pytest
import os
import io
import time
import json
import logging
import boto3
import pandas as pd

from sqlalchemy import create_engine,text
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.serialization import SerializationContext, MessageField, StringDeserializer, Deserializer
 
from typing import Optional
 
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
    value = os.getenv(name, default)

    logger.info(f"{name}: {value}")
    return value
 
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
                    value = secret["value"]
                    logger.info(f"{name}: *****")
                    return value
                else:
                    break
    return default
 
# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
FILTER_CATALOG = get_param('FILTER_CATALOG', "")
FILTER_SCHEMA = get_param('FILTER_SCHEMA', "")
FILTER_TABLE = get_param('FILTER_TABLE', "")
FILTER_BATCH = get_param('FILTER_BATCH', "")
 
TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')
 
KAFKA_BOOTSTRAP_SERVERS = get_param('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_SECURITY_PROTOCOL = get_param('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
KAFKA_SSL_CA_LOCATION = get_param('KAFKA_SSL_CA_LOCATION', '/path/to/ca.pem')
KAFKA_SSL_CERT_LOCATION = get_param('KAFKA_SSL_CERT_LOCATION', '/path/to/client_cert.pem')
KAFKA_SSL_KEY_LOCATION = get_param('KAFKA_SSL_KEY_LOCATION', '/path/to/client_key.pem')
KAFKA_SSL_KEY_PASSWORD = get_credential('KAFKA_SSL_KEY_PASSWORD', '<PASSWORD_NOT_SET>')  # if your key is password protected
KAFKA_SASL_USERNAME = get_credential('KAFKA_SASL_USERNAME', '<USERNAME_NOT_SET>')
KAFKA_SASL_PASSWORD = get_credential('KAFKA_SASL_PASSWORD', '<PASSWORD_NOT_SET>')
KAFKA_TOPIC_NAME = get_param('KAFKA_TOPIC_NAME', 'dpraw_execution_status_log_v1')
 
# Connect to MinIO or AWS S3
ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')
S3_ADMIN_BUCKET = get_param('S3_ADMIN_BUCKET', 'admin-bucket')
S3_LOCATION_LIST_OBJECT_NAME = get_param('S3_LOCATION_LIST_OBJECT_NAME', 's3_locations.csv')

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
AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', None)
AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', None)

if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    s3_config["aws_access_key_id"] = AWS_ACCESS_KEY
    s3_config["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
if ENDPOINT_URL:
    s3_config["endpoint_url"] = ENDPOINT_URL
    s3_config["verify"] = False  # Disable SSL verification for self-signed certificates

s3 = boto3.client(**s3_config)
 
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

def get_s3_location_list(batch: str) -> pd.DataFrame:
    """
    Retrieves a list of S3 locations from a CSV file stored in an S3 bucket and returns it as a pandas DataFrame.
    Parameters:
        batch (str): The name of the S3 batch to filter the locations by. If provided, only locations matching this batch are returned.
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

def get_baseline(table):
    return latest_values[table]
 
def get_actual_count(table: str, timestamp_column: Optional[str] = None, baseline_timestamp: Optional[int] = None) -> int:
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
        
        logger.info(f"Executing query: {query}")
        try:
            count = conn.execute(query).scalar()
        except Exception as e:
            logger.error(f"Error executing query for table '{table}': {str(e)}")
            count = None
    return count
 
def init_actual_values_from_kafka(filter_catalog: Optional[str] = None, filter_schema: Optional[str] = None, filter_table: Optional[str] = None, filter_batch: Optional[str] = None):
    """
    Initializes and returns the latest actual values for tables by consuming messages from a Kafka topic.
    This function reads String-serialized messages from a Kafka topic, optionally filtering by catalog, schema, and table name.
    For each table, only the message with the latest timestamp is retained. The result is a dictionary mapping fully qualified
    table names to their latest metric values and associated metadata.
    Args:
        filter_catalog (str, optional): If provided, only messages with this catalog are processed.
        filter_schema (str, optional): If provided, only messages with this schema are processed.
        filter_table (str, optional): If provided, only messages with this table name are processed.
    Returns:
        dict: A dictionary where keys are fully qualified table names (catalog.schema.table_name) and values are dictionaries
              containing the latest timestamp, timestamp_column, value, metric_type, and table_name.
    Raises:
        KeyboardInterrupt: If the consumer is interrupted by the user.
    Side Effects:
        Prints status and debug information to stdout.
        Closes the Kafka consumer upon completion.
    """
    latest_values: dict[str, dict] = {}
    logger.info(f"running init_actual_values_from_kafka({filter_catalog},{filter_schema},{filter_table})")
 
    # the list of S3 locations with the tables (potentially filtered by batch)
    s3_location_list = get_s3_location_list(filter_batch)

    # read from kafka
    if KAFKA_SECURITY_PROTOCOL == 'SSL':
        consumer = Consumer(consumer_conf_ssl)
    elif KAFKA_SECURITY_PROTOCOL == 'SASL_SSL':
        consumer = Consumer(consumer_conf_sasl_ssl)
    else:
        consumer = Consumer(consumer_conf_plaintext)
 
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
                data:dict = json.loads(msg.value().decode("utf-8"))
 
                # find the post_create_table_metric job
                if data["steps"] and "post_create_table_metric" in data["steps"]:
                    post_create_table_metric_step: dict = data["steps"]["post_create_table_metric"]

                    if "job_outcome" in post_create_table_metric_step and post_create_table_metric_step["job_outcome"] == "SUCCESS":                   
#                        logger.info(f"Received message with post_create_table_metric_step: {post_create_table_metric_step['dp_controller_response_message']}")

                        metric: dict = json.loads(post_create_table_metric_step["job_exit_message"])
 
                        # apply filters it set
                        if filter_catalog and metric.get('catalog') != filter_catalog:
                            continue
                        if filter_schema and metric.get('schema') != filter_schema:
                            continue
                        if filter_table and metric.get('table_name') != filter_table:
                            continue

                        # check if the table is in the list of S3 locations and if not skip it
                        fully_qualified_table_name = f"{metric.get('schema')}.{metric.get('table_name')}"
                        if fully_qualified_table_name not in s3_location_list["fully_qualified_table_name"].values:
                            logger.info(f"Skipping table {fully_qualified_table_name} as it is not in the S3 location list")
                            continue

                        # build key of dictionary with the fully qualified table name
                        key = f"{metric.get('catalog')}.{metric.get('schema')}.{metric.get('table_name')}"
                        timestamp = metric.get('event_time', 0)  # Assuming event_time is in milliseconds
    
                        # Store only the latest value based on timestamp
                        if key:
                            if key not in latest_values or timestamp > latest_values[key]['timestamp']:
                                latest_values[key] = {
                                    'timestamp': timestamp,
                                    'timestamp_column': metric.get('timestamp_column', None),
                                    'count': metric.get('count', 0),
                                    'table_name': metric.get('table_name', key)
                                }
                                logger.info(f"Updated latest value for {key}: {latest_values[key]}")
                            else:
                                logger.info(f"Skipped older message for {key}: timestamp {timestamp} <= {latest_values[key]['timestamp']}")
 
    except KeyboardInterrupt:
        logger.info("Stopping consumer.")
    except Exception as ex:
        logger.error(f"Error {ex}")
    finally:
        consumer.close()
        logger.info(f"init_actual_values_from_kafka() completed. Found {len(latest_values)} tables: {list(latest_values.keys())}")
    return latest_values

# all the latest values from Kafka (applying a potential filer set via environment variables)
latest_values = init_actual_values_from_kafka(FILTER_CATALOG, FILTER_SCHEMA, FILTER_TABLE, FILTER_BATCH)
 
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
def test_value_compare(fully_qualified_table_name):
    """
    Test function to compare the value counts between baseline and actual data for a given table.
 
    This test is parameterized to run for each table in the `tables` list. For each table:
    - Retrieves the baseline count, timestamp column, and event time using `get_baseline`.
    - Retrieves the actual count from the target data source using `get_actual_count`, filtered by the timestamp column and baseline timestamp.
    - Asserts that the baseline and actual counts are equal, raising an assertion error with a descriptive message if they do not match.
 
    Args:
        table (str): The name of the table to compare.
 
    Raises:
        AssertionError: If the baseline count does not match the actual count for the given table.
    """
    baseline_count = get_baseline(fully_qualified_table_name)['count']
    timestamp_column = get_baseline(fully_qualified_table_name)['timestamp_column']
    event_time = get_baseline(fully_qualified_table_name)['timestamp']
    actual_count = get_actual_count(fully_qualified_table_name, timestamp_column=timestamp_column, baseline_timestamp=event_time)
 
    assert baseline_count == actual_count, f"Mismatch in table '{fully_qualified_table_name}': {baseline_count} != {actual_count}"
 