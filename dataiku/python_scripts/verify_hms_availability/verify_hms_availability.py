import sys
import os
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

sys.path.append('gen-py')

from hive_metastore import ThriftHiveMetastore

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
HMS_HOST = get_param('HMS_HOST', 'localhost')
HMS_PORT = get_param('HMS_PORT', '9083')
HMS_TEMP_TABLE_NAME = get_param('HMS_TEMP_TABLE_NAME', 'hms_test_availability_t')
HMS_TEMP_TABLE_DBNAME = get_param('HMS_TEMP_TABLE_DBNAME', 'default')
HMS_TEMP_TABLE_LOCATION = get_param('HMS_TEMP_TABLE_LOCATION', 's3a://flight-bucket/hms_test_availability/')

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
        tableName=HMS_TEMP_TABLE_NAME,
        dbName=HMS_TEMP_TABLE_DBNAME,
        tableType='EXTERNAL_TABLE',

        sd=ThriftHiveMetastore.StorageDescriptor(
            cols=fields,
            location=HMS_TEMP_TABLE_LOCATION,
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
    created_table = client.get_tables(HMS_TEMP_TABLE_DBNAME, HMS_TEMP_TABLE_NAME)
    assert len(created_table) > 0, f"Table '{HMS_TEMP_TABLE_NAME}' was not created successfully"
    assert created_table, f"Table '{HMS_TEMP_TABLE_NAME}' was not created successfully"

    # Close connection
    transport.close()    


def test_drop_table():
    client, transport = getClient()

    client.drop_table(HMS_TEMP_TABLE_DBNAME, HMS_TEMP_TABLE_NAME, True)

    # Verify table creation
    created_table = client.get_tables(HMS_TEMP_TABLE_DBNAME, HMS_TEMP_TABLE_NAME)
    assert len(created_table) == 0, f"Table '{HMS_TEMP_TABLE_NAME}' was not created successfully"

    # Close connection
    transport.close()  