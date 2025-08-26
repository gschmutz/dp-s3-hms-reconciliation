import pytest
import os
import time
import json
import logging

from sqlalchemy import create_engine,text
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.serialization import SerializationContext, MessageField, StringDeserializer, Deserializer

from typing import Optional

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

# Environment variables for setting the filter to apply when reading the baseline counts from Kafka. If not set (left to default) then all the tables will consumed and compared against actual counts.
FILTER_CATALOG = get_param('FILTER_CATALOG', None)
FILTER_SCHEMA = get_param('FILTER_SCHEMA', None)
FILTER_TABLE = get_param('FILTER_TABLE', None)

TRINO_USER = os.getenv('TRINO_USER', 'trino')
TRINO_PASSWORD = os.getenv('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_SCHEMA = get_param('TRINO_SCHEMA', 'flight_db')

KAFKA_BOOTSTRAP_SERVERS = get_param('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_SECURITY_PROTOCOL = get_param('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
KAFKA_SSL_CA_LOCATION = get_param('KAFKA_SSL_CA', '/path/to/ca.pem')
KAFKA_SSL_CERT_LOCATION = get_param('KAFKA_SSL_CERT', '/path/to/client_cert.pem')
KAFKA_SSL_KEY_LOCATION = get_param('KAFKA_SSL_KEY', '/path/to/client_key.pem')
KAFKA_SSL_KEY_PASSWORD = get_param('KAFKA_SSL_KEY_PASSWORD', '<PASSWORD_NOT_SET>')  # if your key is password protected
KAFKA_SASL_USERNAME = get_param('KAFKA_SASL_USERNAME', '<USERNAME_NOT_SET>')
KAFKA_SASL_PASSWORD = get_param('KAFKA_SASL_PASSWORD', '<PASSWORD_NOT_SET>')
KAFKA_TOPIC_NAME = get_param('KAFKA_TOPIC_NAME', 'dpraw_execution_status_log_v1')

# Construct connection URLs
trino_url = f'trino://{TRINO_USER}:{TRINO_PASSWORD}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}/{TRINO_SCHEMA}'

# Setup connections
trino_engine = create_engine(trino_url)

# Kafka Consumer config
consumer_conf_ssl = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'avro-consumer-group',
    'auto.offset.reset': 'earliest',
    'key.deserializer': StringDeserializer("utf-8"),
    'value.deserializer': StringDeserializer("utf-8"),
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
    'group.id': 'avro-consumer-group',
    'auto.offset.reset': 'earliest',
    'key.deserializer': StringDeserializer("utf-8"),
    'value.deserializer': StringDeserializer("utf-8"),
    'enable.auto.commit': True,
    'enable.partition.eof': True,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': KAFKA_SASL_USERNAME,
    'sasl.password': KAFKA_SASL_PASSWORD,
    'ssl.ca.location': KAFKA_SSL_CA_LOCATION,
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

            logger.info(f"Executing query: {query}")
            count = conn.execute(query).scalar()
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
            count = conn.execute(query).scalar()
    return count

def init_actual_values_from_kafka(filter_catalog: Optional[str] = None, filter_schema: Optional[str] = None, filter_table: Optional[str] = None):
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
    print ("running init")
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
    partitions = metadata.topics[topic].partitions
    topic_partitions = [TopicPartition(topic, p) for p in partitions]

    consumer.assign(topic_partitions)

    time.sleep(2)  # wait for messages to be available
    
    # Seek to the beginning offset for each partition
    for tp in topic_partitions:
        consumer.seek(TopicPartition(topic, tp.partition, offset=0))

    try:
        print(f"Consuming messages from topic: {topic}")
        eof_count = 0
    
        while True:
            msg = consumer.poll(timeout = 2.0)
            if msg is None:
                # No message in this poll
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    eof_count += 1
                    # Optional: stop when all partitions have been consumed

                    print(f"EOF count: {eof_count}, Total partitions: {len(topic_partitions)}")

                    if eof_count == len(topic_partitions):
                        print("All partitions consumed.")
                        break
                else:                     
                    print("Consumer error: {}".format(msg.error()))
                    continue
            else:
                eof_count = 0
                # deserialize
                data:dict = json.loads(msg.value().decode("utf-8"))

                # find the post_create_table_metric job
                post_create_table_metric_step: dict = data["steps"]["post_create_table_metric"]
                metric: dict = json.loads(post_create_table_metric_step["dp_controller_response_message"])

                print(f"Received message: {metric}")

                # apply filters it set
                if filter_catalog and metric.get('catalog') != filter_catalog:
                    continue
                if filter_schema and metric.get('schema') != filter_schema:
                    continue
                if filter_table and metric.get('table_name') != filter_table:
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
                        print(f"Updated latest value for {key}: {latest_values[key]}")
                    else:
                        print(f"Skipped older message for {key}: timestamp {timestamp} <= {latest_values[key]['timestamp']}")

    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        consumer.close()
        print(f"Init completed. Found {len(latest_values)} tables: {list(latest_values.keys())}")
    return latest_values

# all the latest values from Kafka (applying a potential filer set via environment variables)
latest_values = init_actual_values_from_kafka(FILTER_CATALOG, FILTER_SCHEMA, FILTER_TABLE)

# collection of fully qualified table names
fully_qualified_table_names = list(latest_values.keys())

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