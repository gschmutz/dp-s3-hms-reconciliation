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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, replace_vars_in_string

sys.path.append('gen-py')
 
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

    tables = conn.execute(text(f"SHOW TABLES IN information_schema")).fetchall()
    # just check if we have tables
    assert len(tables), f"No tables have been found in schema 'information_schema', should be more than 0"

    # Close connection
    conn.close()

def test_create_table():
    """Test disabled - it is not possible to create the table via Trino."""
    pass
    
    # conn = getTrinoConnection()

    # conn.execute(text(f"""
    #         CREATE TABLE {TEMP_TABLE_DBNAME}.{TEMP_TABLE_NAME} (
    #             id         INTEGER,
    #             name       VARCHAR
    #         )
    #         WITH (
    #             external_location = '{TEMP_TABLE_LOCATION}',
    #             format = 'PARQUET'
    #         )
    #     """))

    # assert exists(TEMP_TABLE_DBNAME, TEMP_TABLE_NAME), f"Table {TEMP_TABLE_DBNAME}.{TEMP_TABLE_NAME} should exist after creation"

    # # Close connection
    # conn.close()    


def test_drop_table():
    conn = getTrinoConnection() 

    conn.execute(text(f"""
            DROP TABLE IF EXISTS {TEMP_TABLE_DBNAME}.{TEMP_TABLE_NAME}
            """))

    assert not exists(TEMP_TABLE_DBNAME, TEMP_TABLE_NAME), f"Table {TEMP_TABLE_DBNAME}.{TEMP_TABLE_NAME} should not exist after drop"
    # Close connection
    conn.close()        