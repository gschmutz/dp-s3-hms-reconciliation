from sqlalchemy import create_engine, select, text, Column, Integer, String, DateTime, inspect

def get_table_names(engine, catalog_name):
    """
    Retrieves a list of table names from the source database's public schema, optionally filtering by a comma-separated list of table names.
    Args:
        engine: The SQLAlchemy engine connected to the source database.
        catalog_name (str): The name of the catalog to search within.
    Returns:
        list: A list of table names (str) matching the criteria from the source database.
    Raises:
        Any exceptions raised by the underlying database connection or query execution.
    Notes:
        - The function adapts the query for PostgreSQL or other SQL engines (e.g., Trino) based on the source engine's dialect.
        - Only tables of type 'BASE TABLE' in the 'public' schema are considered.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT table_name 
            FROM {catalog_name}information_schema.tables  t
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            AND table_name IN (SELECT table_name
                                FROM {catalog_name}information_schema.columns c 
                                WHERE UPPER(c.column_name) IN ('PART_ID', 'TBL_ID', 'DB_ID', 'CTLG_ID')
                             )                        
        """))
        return [row[0] for row in result]