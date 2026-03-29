"""
Snowflake utility functions for Boston Reliability Engine
Reuses existing utilities from include.ingestion.snowflake_queries
"""
import os
from dotenv import load_dotenv
from include.ingestion.snowflake_queries import (
    get_snowpark_session as _get_snowpark_session,
    execute_snowflake_query as _execute_snowflake_query
)

load_dotenv()


def get_snowpark_session(schema=None):
    """
    Create Snowpark session using existing utility from eczachly.

    Args:
        schema: Snowflake schema to use (defaults to STUDENT_SCHEMA env var)

    Returns:
        snowflake.snowpark.Session
    """
    schema = schema or os.getenv("STUDENT_SCHEMA", "bootcamp")
    return _get_snowpark_session(schema=schema)


def execute_snowflake_query(query, fetch_results=True):
    """
    Execute a Snowflake query using existing utility from eczachly.

    Note: The underlying eczachly function always fetches results.
    Use this for DDL (CREATE TABLE, etc.) or DML (INSERT, UPDATE).

    Args:
        query: SQL query string
        fetch_results: Whether to return results (default: True)

    Returns:
        List of result rows if fetch_results=True, else None
    """
    result = _execute_snowflake_query(query)
    return result if fetch_results else None


def create_bronze_table_if_not_exists(table_name, columns_def, schema=None):
    """
    Create a bronze layer table if it doesn't exist.

    Args:
        table_name: Name of the table to create
        columns_def: Column definitions (e.g., "id VARCHAR, name VARCHAR, created_at TIMESTAMP")
        schema: Snowflake schema (defaults to STUDENT_SCHEMA)
    """
    schema = schema or os.getenv("STUDENT_SCHEMA")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        {columns_def},
        _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """

    execute_snowflake_query(create_table_sql, fetch_results=False)
    print(f"Table {schema}.{table_name} created or already exists")
