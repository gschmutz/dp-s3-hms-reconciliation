import sys
import os
import logging
from typing import Optional
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
# either postgresql or trino
METASTORE_DB_ACCESS_STRATEGY = get_param('METASTORE_DB_ACCESS_STRATEGY', 'postgresql')

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
HMS_TRINO_SCHEMA = get_param('HMS_TRINO_SCHEMA', 'flight_db')

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_SCHEMA = get_param('TRINO_SCHEMA', 'flight_db')

# Construct connection URLs
hms_db_url = f'postgresql://{HMS_DB_USER}:{HMS_DB_PASSWORD}@{HMS_DB_HOST}:{HMS_DB_PORT}/{HMS_DB_DBNAME}'
# Construct connection URLs
hms_trino_url = f'trino://{HMS_TRINO_USER}:{HMS_TRINO_PASSWORD}@{HMS_TRINO_HOST}:{HMS_TRINO_PORT}/{HMS_TRINO_CATALOG}/{HMS_TRINO_SCHEMA}'

# Construct connection URLs
trino_url = f'trino://{TRINO_USER}:{TRINO_PASSWORD}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}/{TRINO_SCHEMA}'

# Setup connections to the metadatastore, either directly to postgresql or via trino
if METASTORE_DB_ACCESS_STRATEGY.lower() == 'postgresql':
    hms_engine = create_engine(hms_db_url)
else:
    hms_engine = create_engine(hms_trino_url)
    # You can get the dialect name (db type) from the engine

trino_engine = create_engine(trino_url)

def get_tables(database_name: Optional[str] = None):
    if hms_engine.dialect.name == 'postgresql':
        catalog_name = ""
    else:
        catalog_name = "hive_metastore_db."

    with hms_engine.connect() as conn:
        # TODO: Make end_timestamp optional and configure number of seconds to add
        sql = f"""
            SELECT d."NAME" as database_name,
               t."TBL_ID",
               t."CREATE_TIME",
               t."TBL_NAME",
               t."TBL_TYPE"
            FROM {catalog_name}public."TBLS" t
            JOIN {catalog_name}public."DBS" d ON t."DB_ID" = d."DB_ID"
        """
        if (database_name is not None) and (database_name != ""):
            sql += f" WHERE d.\"NAME\" = '{database_name}'"

        result = conn.execute(text(sql))

        return result.mappings().all()

def do_trino_repair(database_name: Optional[str] = None):
    all_tables = get_tables(database_name)

    for table in all_tables:
        table_name = table["TBL_NAME"]
        database = table["DATABASE_NAME"]
        with trino_engine.connect() as conn:
            conn.execute(text(f"call minio.system.sync_partition_metadata('{database}', '{table_name}', 'FULL')"))

if __name__ == "__main__":
    do_trino_repair()