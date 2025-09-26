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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, replace_vars_in_string

sys.path.append('gen-py')
 
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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