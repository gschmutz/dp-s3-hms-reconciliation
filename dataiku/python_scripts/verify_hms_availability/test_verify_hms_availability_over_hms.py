"""
This script verifies the availability and basic operations of the Hive Metastore (HMS) using Thrift. It is designed to run both inside and outside Dataiku DSS scenarios, supporting dynamic configuration via scenario variables or environment variables.
Main functionalities:
- Establishes a Thrift connection to the Hive Metastore using configurable host and port.
- Retrieves configuration parameters and credentials from Dataiku scenario variables or environment variables.
- Provides test functions to:
    - Check available catalogs (`test_get_catalogs`)
    - List all databases (`test_get_databases`)
    - List all tables in non-default databases (`test_get_tables`)
    - Create a temporary external table for testing (`test_create_table`)
    - Drop the temporary test table (`test_drop_table`)
Environment variables or scenario parameters used:
- HMS_HOST: Hostname of the Hive Metastore (default: 'localhost')
- HMS_PORT: Port of the Hive Metastore (default: '9083')
- TEMP_TABLE_NAME: Name of the temporary test table (default: 'hms_test_availability_t')
- TEMP_TABLE_DBNAME: Database name for the test table (default: 'default')
- TEMP_TABLE_LOCATION: S3 location for the test table (default: 's3a://flight-bucket/hms_test_availability/')
The script is intended for use in automated testing or monitoring scenarios to ensure HMS is available and operational.
"""
import sys
import os
import logging
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hive_metastore import ThriftHiveMetastore

sys.path.append('gen-py')
 
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

# Environment variables 
HMS_HOST = get_param('HMS_HOST', 'localhost')
HMS_PORT = get_param('HMS_PORT', '9083')
TEMP_TABLE_NAME = get_param('TEMP_TABLE_NAME', 'hms_test_availability_t')
TEMP_TABLE_DBNAME = get_param('TEMP_TABLE_DBNAME', 'default')
TEMP_TABLE_LOCATION = get_param('TEMP_TABLE_LOCATION', 's3a://flight-bucket/hms_test_availability/')

def getClient():
    # Connect
    transport = TSocket.TSocket(HMS_HOST, HMS_PORT)  # change to your metastore host
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = ThriftHiveMetastore.Client(protocol)
    transport.open()
    return client, transport


def test_get_catalogs():
    client, transport = getClient()
    
    catalogs = client.get_catalogs()
    
    # just check if we have catalogs
    assert len(catalogs.names), f"No catalogs have been found, should be more than 0"

    # Close connection
    transport.close()

def test_get_databases():
    client, transport = getClient()
    
    databases = client.get_all_databases()

    # just check if we have databases    
    assert len(databases), f"No databases have been found, should be more than 0"

    # Close connection
    transport.close()


def test_get_tables():
    client, transport = getClient()

    databases = client.get_all_databases()

    for db in databases:
        if db == 'default':
            # Skip default database
            continue
        tables = client.get_all_tables(db)
        # just check if we have tables
        assert len(tables), f"No tables have been found in database '{db}', should be more than 0"

    # Close connection
    transport.close()


def test_create_table():
    client, transport = getClient()

    fields = [
        ThriftHiveMetastore.FieldSchema(name='id', type='int', comment='ID'),
        ThriftHiveMetastore.FieldSchema(name='name', type='string', comment='Name'),
    ]
    table_def = ThriftHiveMetastore.Table(
        tableName=TEMP_TABLE_NAME,
        dbName=TEMP_TABLE_DBNAME,
        tableType='EXTERNAL_TABLE',

        sd=ThriftHiveMetastore.StorageDescriptor(
            cols=fields,
            location=TEMP_TABLE_LOCATION,
            inputFormat='org.apache.hadoop.mapred.TextInputFormat',
            outputFormat='org.apache.hadoop.mapred.TextInputFormat',
            serdeInfo=ThriftHiveMetastore.SerDeInfo(
                name='test_serde',
                serializationLib='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                parameters={}
            )
        )
    )
    
    client.create_table(table_def)

    # Verify table creation
    created_table = client.get_tables(TEMP_TABLE_DBNAME, TEMP_TABLE_NAME)
    assert len(created_table) > 0, f"Table '{TEMP_TABLE_NAME}' was not created successfully"
    assert created_table, f"Table '{TEMP_TABLE_NAME}' was not created successfully"

    # Close connection
    transport.close()    


def test_drop_table():
    client, transport = getClient()

    client.drop_table(TEMP_TABLE_DBNAME, TEMP_TABLE_NAME, True)

    # Verify table creation
    created_table = client.get_tables(TEMP_TABLE_DBNAME, TEMP_TABLE_NAME)
    assert len(created_table) == 0, f"Table '{TEMP_TABLE_NAME}' was not created successfully"

    # Close connection
    transport.close()  