"""
This module provides utility functions and tests for verifying HMS (Hive Metastore) availability over Trino using SQLAlchemy.
It is designed to run both inside and outside Dataiku DSS scenarios, supporting dynamic parameter and credential retrieval
from Dataiku scenario variables, Dataiku secrets, or environment variables.
Functions:
    - get_param(name, default=None): Retrieve a parameter value from scenario variables or environment variables.
    - get_credential(name, default=None): Retrieve a secret credential from Dataiku secrets or environment variables.
    - getTrinoConnection(): Establish and return a SQLAlchemy connection to Trino using configured parameters.
    - exists(dbname, table_name): Check if a table exists in the specified database/schema in Trino.
    - test_get_catalogs(): Test that catalogs are available in Trino.
    - test_get_schemas(): Test that schemas are available in Trino.
    - test_get_tables(): Test that tables exist in non-default schemas in Trino.
    - test_create_table(): Test creation of a temporary table in Trino and verify its existence.
    - test_drop_table(): Test dropping of a temporary table in Trino and verify its non-existence.
Environment Variables / Scenario Variables:
    - TRINO_USER: Trino username (credential).
    - TRINO_PASSWORD: Trino password (credential).
    - TRINO_HOST: Trino host address.
    - TRINO_PORT: Trino port.
    - TRINO_CATALOG: Trino catalog name.
    - TRINO_USE_SSL: Whether to use SSL for Trino connection.
    - TEMP_TABLE_NAME: Name of the temporary table for testing.
    - TEMP_TABLE_DBNAME: Database/schema for the temporary table.
    - TEMP_TABLE_LOCATION: External location for the temporary table (S3 path).
Logging:
    - Logs parameter and credential retrieval, as well as connection and test status.
Intended Usage:
    - For automated testing of HMS availability and Trino connectivity, especially in Dataiku DSS environments.

"""
import sys
import os
import logging
from sqlalchemy import create_engine,text

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

# Environment variables 
TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')

TEMP_TABLE_NAME = get_param('TEMP_TABLE_NAME', 'hms_test_availability_t')
TEMP_TABLE_DBNAME = get_param('TEMP_TABLE_DBNAME', 'default')
TEMP_TABLE_LOCATION = get_param('TEMP_TABLE_LOCATION', 's3a://flight-bucket/hms_test_availability/')

trino_url = f'trino://{TRINO_USER}:{TRINO_PASSWORD}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}'
if TRINO_USE_SSL:
    trino_url = f'{trino_url}?protocol=https&verify=false'

def getTrinoConnection():
    """
    Establishes and returns a connection to a Trino database using the specified Trino URL.
    Returns:
        sqlalchemy.engine.Connection: An active connection object to the Trino database.
    Raises:
        sqlalchemy.exc.SQLAlchemyError: If the connection cannot be established.
    """
    trino_engine = create_engine(trino_url)
    conn = trino_engine.connect()

    return conn

def exists(dbname, table_name):
    """
    Checks if a table exists in the specified database using a Trino connection.

    Args:
        dbname (str): The name of the database to search in.
        table_name (str): The name of the table to check for existence.

    Returns:
        bool: True if the table exists in the database, False otherwise.
    """
    conn = getTrinoConnection()

    tables = conn.execute(text(f"SHOW TABLES IN {dbname}")).fetchall()

    for table in tables:
        if table[0] == table_name:
            return True

    # Close connection
    conn.close()
    return False

def test_get_catalogs():
    conn = getTrinoConnection()

    catalogs = conn.execute(text("SHOW CATALOGS")).fetchall()

    # just check if we have catalogs
    assert len(catalogs), f"No catalogs have been found, should be more than 0"

    # Close connection
    conn.close()

def test_get_schemas():
    conn = getTrinoConnection()

    schemas = conn.execute(text("SHOW SCHEMAS")).fetchall()

    # just check if we have schemas
    assert len(schemas), f"No schemas have been found, should be more than 0"

    # Close connection
    conn.close()

def test_get_tables():
    conn = getTrinoConnection()

    schemas = conn.execute(text("SHOW SCHEMAS")).fetchall()

    for schema in schemas:
        if not schema:
            continue
        if schema[0] == 'default':
            # Skip default schema
            continue
        tables = conn.execute(text(f"SHOW TABLES IN {schema[0]}")).fetchall()
        # just check if we have tables
        assert len(tables), f"No tables have been found in schema '{schema}', should be more than 0"

    # Close connection
    conn.close()

def test_create_table():
    conn = getTrinoConnection()

    conn.execute(text(f"""
            CREATE TABLE {TEMP_TABLE_DBNAME}.{TEMP_TABLE_NAME} (
                id         INTEGER,
                name       VARCHAR
            )
            WITH (
                external_location = '{TEMP_TABLE_LOCATION}',
                format = 'PARQUET'
            )
        """))

    assert exists(TEMP_TABLE_DBNAME, TEMP_TABLE_NAME), f"Table {TEMP_TABLE_DBNAME}.{TEMP_TABLE_NAME} should exist after creation"

    # Close connection
    conn.close()    


def test_drop_table():
    conn = getTrinoConnection() 

    conn.execute(text(f"""
            DROP TABLE IF EXISTS {TEMP_TABLE_DBNAME}.{TEMP_TABLE_NAME}
            """))

    assert not exists(TEMP_TABLE_DBNAME, TEMP_TABLE_NAME), f"Table {TEMP_TABLE_DBNAME}.{TEMP_TABLE_NAME} should not exist after drop"
    # Close connection
    conn.close()        