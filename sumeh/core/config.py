#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module provides a set of utility functions to retrieve and parse configuration data
from various data sources, including S3, MySQL, PostgreSQL, BigQuery, CSV files, AWS Glue
Data Catalog, DuckDB, and Databricks. Additionally, it includes functions to infer schema
information from these sources.

Functions:
    get_config_from_s3(s3_path: str, delimiter: Optional[str] = ",") -> List[Dict[str, Any]]:

    get_config_from_mysql(...) -> List[Dict[str, Any]]:

    get_config_from_postgresql(...) -> List[Dict[str, Any]]:

    get_config_from_bigquery(...) -> List[Dict[str, str]]:

    get_config_from_csv(file_path: str, delimiter: Optional[str] = ",") -> List[Dict[str, str]]:
        Retrieves configuration data from a local CSV file.

    get_config_from_glue_data_catalog(...) -> List[Dict[str, str]]:

    get_config_from_duckdb(...) -> List[Dict[str, Any]]:
        Retrieves configuration data from a DuckDB database.

    get_config_from_databricks(...) -> List[Dict[str, Any]]:
        Retrieves configuration data from a Databricks table.

    get_schema_from_csv(file_path: str, delimiter: str = ",", sample_size: int = 1_000) -> List[Dict[str, Any]]:
        Infers the schema of a CSV file based on its content.

    get_schema_from_s3(s3_path: str, **kwargs) -> List[Dict[str, Any]]:
        Infers the schema of a CSV file stored in S3.

    get_schema_from_mysql(...) -> List[Dict[str, Any]]:
        Retrieves schema information from a MySQL database table.

    get_schema_from_postgresql(...) -> List[Dict[str, Any]]:
        Retrieves schema information from a PostgreSQL database table.

    get_schema_from_bigquery(...) -> List[Dict[str, Any]]:
        Retrieves schema information from a Google BigQuery table.

    get_schema_from_glue(...) -> List[Dict[str, Any]]:
        Retrieves schema information from AWS Glue Data Catalog.

    get_schema_from_duckdb(...) -> List[Dict[str, Any]]:
        Retrieves schema information from a DuckDB database table.

    get_schema_from_databricks(...) -> List[Dict[str, Any]]:
        Retrieves schema information from a Databricks table.

    __read_s3_file(s3_path: str) -> Optional[str]:

    __parse_s3_path(s3_path: str) -> Tuple[str, str]:

    __read_local_file(file_path: str) -> str:

    __read_csv_file(file_content: str, delimiter: Optional[str] = ",") -> List[Dict[str, str]]:

    __parse_data(data: list[dict]) -> list[dict]:
        Parses the configuration data into a structured format.

    __create_connection(connect_func, host, user, password, database, port) -> Any:

    infer_basic_type(val: str) -> str:
        Infers the basic data type of given value.
"""
from io import StringIO
from dateutil import parser
from typing import List, Dict, Any, Tuple, Optional
from datetime import date, datetime


def get_config_from_s3(s3_path: str, delimiter: Optional[str] = ","):
    """
    Retrieves configuration data from a CSV file stored in an S3 bucket.

    Args:
        s3_path (str): The S3 path to the CSV file.
        delimiter (Optional[str]): The delimiter used in the CSV file (default is ",").

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the parsed configuration data.

    Raises:
        RuntimeError: If there is an error reading or processing the S3 file.
    """
    try:
        file_content = __read_s3_file(s3_path)
        data = __read_csv_file(file_content, delimiter)
        return __parse_data(data)

    except Exception as e:
        raise RuntimeError(f"Error reading or processing the S3 file: {e}")


def get_config_from_mysql(
    host: str = None,
    user: str = None,
    password: str = None,
    database: str = None,
    schema: str = None,
    table: str = None,
    port: int = 3306,
    query: str = None,
    conn=None,
) -> List[Dict[str, Any]]:
    """
    Get configuration from MySQL table

    Args:
        host: MySQL host (not needed if conn is provided)
        user: MySQL user (not needed if conn is provided)
        password: MySQL password (not needed if conn is provided)
        database: Database name (not needed if conn is provided)
        schema: Schema name (optional)
        table: Table name to query
        port: MySQL port (default: 3306)
        query: Optional custom query (if not provided, uses schema and table)
        conn: Existing MySQL connection (optional)

    Returns:
        List of dicts with configuration data
    """
    import mysql.connector

    if conn is None:
        if not all([host, user, password, database]):
            raise ValueError(
                "Either 'conn' or all of (host, user, password, database) must be provided"
            )
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database, port=port
        )
        should_close = True
    else:
        should_close = False

    cursor = conn.cursor(dictionary=True)

    if query is None:
        if schema and table:
            query = f"SELECT * FROM {schema}.{table}"
        elif table:
            query = f"SELECT * FROM {table}"
        else:
            raise ValueError("Either 'query' or 'table' must be provided")

    cursor.execute(query)
    config_data = cursor.fetchall()
    cursor.close()

    if should_close:
        conn.close()

    if not config_data:
        raise ValueError(f"No configuration data found with query: {query}")

    return __parse_data(config_data)


def get_config_from_postgresql(
    connection: Optional = None,
    host: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    port: Optional[int] = 5432,
    schema: Optional[str] = None,
    table: Optional[str] = None,
    query: Optional[str] = None,
) -> list[dict]:
    """
    Retrieves configuration data from a PostgreSQL database.

    Args:
        connection (Optional): An existing PostgreSQL connection object.
        host (Optional[str]): Host of the PostgreSQL server.
        user (Optional[str]): Username to connect to PostgreSQL.
        password (Optional[str]): Password for the PostgreSQL user.
        database (Optional[str]): Database name to query.
        port (Optional[int]): The port for the PostgreSQL connection (default is 5432).
        schema (Optional[str]): Schema name if query is not provided.
        table (Optional[str]): Table name if query is not provided.
        query (Optional[str]): Custom SQL query to fetch data (if not provided, `schema` and `table` must be given).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the parsed configuration data.

    Raises:
        ValueError: If neither `query` nor both `schema` and `table` are provided.
        ConnectionError: If there is an error connecting to PostgreSQL.
        RuntimeError: If there is an error executing the query or processing the data.
    """
    import psycopg2
    import pandas as pd

    if query is None and (schema is None or table is None):
        raise ValueError(
            "You must provide either a 'query' or both 'schema' and 'table'."
        )

    if query is None:
        query = f"SELECT * FROM {schema}.{table}"

    try:
        connection = connection or __create_connection(
            psycopg2.connect, host, user, password, database, port
        )

        data = pd.read_sql(query, connection)

        data_dict = data.to_dict(orient="records")
        return __parse_data(data_dict)

    except psycopg2.Error as e:
        raise ConnectionError(f"Error connecting to PostgreSQL database: {e}")

    except Exception as e:
        raise RuntimeError(f"Error executing the query or processing data: {e}")

    finally:
        if connection and host is not None:
            connection.close()


def get_config_from_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    credentials_path: Optional[str] = None,
    query: Optional[str] = None,
) -> List[Dict[str, str]]:
    """
    Retrieves configuration data from a Google BigQuery table.

    Args:
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        credentials_path (Optional[str]): Path to service account credentials file (if not provided, defaults to default credentials).
        query (Optional[str]): Custom SQL query to fetch data (if not provided, defaults to SELECT *).

    Returns:
        List[Dict[str, str]]: A list of dictionaries representing the parsed configuration data.

    Raises:
        RuntimeError: If there is an error while querying BigQuery.
    """
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from google.auth.exceptions import DefaultCredentialsError

    if query is None:
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"

    try:
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            client = bigquery.Client(project=project_id)

        data = client.query(query).to_dataframe()
        data_dict = data.to_dict(orient="records")
        return __parse_data(data_dict)

    except DefaultCredentialsError as e:
        raise RuntimeError(f"Credentials error: {e}") from e

    except Exception as e:
        raise RuntimeError(f"Error occurred while querying BigQuery: {e}") from e


def get_config_from_csv(
    file_path: str, delimiter: Optional[str] = ","
) -> List[Dict[str, str]]:
    """
    Retrieves configuration data from a CSV file.

    Args:
        file_path (str): The local file path to the CSV file.
        delimiter (Optional[str]): The delimiter used in the CSV file (default is ",").

    Returns:
        List[Dict[str, str]]: A list of dictionaries representing the parsed configuration data.

    Raises:
        RuntimeError: If there is an error reading or processing the file.
    """
    try:
        file_content = __read_local_file(file_path)
        result = __read_csv_file(file_content, delimiter)

        return __parse_data(result)

    except FileNotFoundError as e:
        raise RuntimeError(f"File '{file_path}' not found. Error: {e}") from e

    except ValueError as e:
        raise ValueError(
            f"Error while parsing CSV file '{file_path}'. Error: {e}"
        ) from e

    except Exception as e:
        # Catch any unexpected exceptions
        raise RuntimeError(
            f"Unexpected error while processing CSV file '{file_path}'. Error: {e}"
        ) from e


def get_config_from_glue_data_catalog(
    glue_context, database_name: str, table_name: str, query: Optional[str] = None
) -> List[Dict[str, str]]:
    """
    Retrieves configuration data from AWS Glue Data Catalog.

    Using Spark directly - works with all table formats (Parquet, ORC, CSV, Iceberg, Delta, Hudi).

    Args:
        glue_context: An instance of `GlueContext`.
        database_name (str): Glue database name.
        table_name (str): Glue table name.
        query (Optional[str]): Custom SQL query to fetch data (if provided).

    Returns:
        List[Dict[str, str]]: A list of dictionaries representing the parsed configuration data.

    Raises:
        RuntimeError: If there is an error querying Glue Data Catalog.
    """
    from awsglue.context import GlueContext

    if not isinstance(glue_context, GlueContext):
        raise ValueError("The provided context is not a valid GlueContext.")

    spark = glue_context.spark_session

    try:
        full_table_name = f"`{database_name}`.`{table_name}`"

        if query:
            data_frame = spark.read.table(full_table_name)
            data_frame.createOrReplaceTempView("temp_table_view")
            data_frame = spark.sql(query)
        else:
            data_frame = spark.read.table(full_table_name)

        data_dict = [row.asDict() for row in data_frame.collect()]

        return __parse_data(data_dict)

    except Exception as e:
        raise RuntimeError(
            f"Error occurred while querying Glue Data Catalog '{database_name}.{table_name}': {e}"
        ) from e


def get_config_from_duckdb(
    table: str = None, query: str = None, conn=None
) -> List[Dict[str, Any]]:
    """
    Retrieve configuration data from a DuckDB database.

    This function fetches data from a DuckDB database either by executing a custom SQL query
    or by selecting all rows from a specified table. The data is then parsed into a list of
    dictionaries.

    Args:
        table (str, optional): The name of the table to fetch data from. Defaults to None.
        query (str, optional): A custom SQL query to execute. Defaults to None.
        conn: A valid DuckDB connection object.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the fetched data.

    Raises:
        ValueError: If neither `table` nor `query` is provided, or if a valid `conn` is not supplied.

    Example:
        >>> import duckdb
        >>> conn = duckdb.connect('my_db.duckdb')
        >>> config = get_config_from_duckdb('my_db.duckdb', table='rules', conn=conn)
    """

    if query:
        df = conn.execute(query).fetchdf()
    elif table:
        df = conn.execute(f"SELECT * FROM {table}").fetchdf()
    else:
        raise ValueError(
            "DuckDB configuration requires:\n"
            "1. Either a `table` name or custom `query`\n"
            "2. A valid database `conn` connection object\n"
            "Example: get_config('duckdb', table='rules', conn=duckdb.connect('my_db.duckdb'))"
        )

    return __parse_data(df.to_dict(orient="records"))


def get_config_from_databricks(
    spark, catalog: Optional[str], schema: Optional[str], table: str, **kwargs
) -> List[Dict[str, Any]]:
    """
    Retrieves configuration data from a Databricks table and returns it as a list of dictionaries.

    Args:
        spark SparkSession: Spark Session to get information from Databricks
        catalog (Optional[str]): The catalog name in Databricks. If provided, it will be included in the table's full path.
        schema (Optional[str]): The schema name in Databricks. If provided, it will be included in the table's full path.
        table (str): The name of the table to retrieve data from.
        query: Additional keyword arguments (currently unused).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents a row of data from the table.
    """

    if catalog and schema:
        full = f"{catalog}.{schema}.{table}"
    elif schema:
        full = f"{schema}.{table}"
    else:
        full = table
    if "query" in kwargs.keys():
        df = spark.sql(f"select * from {full} where {kwargs['query']}")
    else:
        df = spark.table(full)
    return [row.asDict() for row in df.collect()]


def __read_s3_file(s3_path: str) -> Optional[str]:
    """
    Reads the content of a file stored in S3.

    Args:
        s3_path (str): The S3 path of the file.

    Returns:
        str: The content of the S3 file.

    Raises:
        RuntimeError: If there is an error retrieving the file from S3.
    """
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    try:
        s3 = boto3.client("s3")
        bucket, key = __parse_s3_path(s3_path)

        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(
            f"Failed to read file from S3. Path: '{s3_path}'. Error: {e}"
        ) from e

    except UnicodeDecodeError as e:
        raise ValueError(
            f"Failed to decode file content from S3 path '{s3_path}' as UTF-8. Error: {e}"
        ) from e


def __parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """
    Parses an S3 path into its bucket and key components.

    Args:
        s3_path (str): The S3 path to parse. Must start with "s3://".

    Returns:
        Tuple[str, str]: A tuple containing the bucket name and the key.

    Raises:
        ValueError: If the S3 path does not start with "s3://", or if the path
                    format is invalid and cannot be split into bucket and key.
    """
    try:
        if not s3_path.startswith("s3://"):
            raise ValueError("S3 path must start with 's3://'")

        s3_path = s3_path[5:]
        bucket, key = s3_path.split("/", 1)
        return bucket, key

    except ValueError as e:
        raise ValueError(
            f"Invalid S3 path format: '{s3_path}'. Expected format 's3://bucket/key'. Details: {e}"
        ) from e


def __read_local_file(file_path: str) -> str:
    """
    Reads the content of a local file.

    Args:
        file_path (str): The local file path to be read.

    Returns:
        str: The content of the file.

    Raises:
        FileNotFoundError: If the file is not found.
    """
    import os

    # Resolve and validate the file path to prevent path traversal
    resolved_path = os.path.realpath(file_path)

    # Check for path traversal attempts
    if ".." in file_path or not resolved_path.startswith(os.getcwd()):
        raise ValueError(f"Invalid file path: '{file_path}'. Path traversal detected.")

    try:
        with open(resolved_path, mode="r", encoding="utf-8") as file:
            return file.read()
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Error: The file at '{file_path}' was not found."
        ) from e
    except IOError as e:
        raise IOError(f"Error: Could not read file '{file_path}'. Details: {e}") from e


def __read_csv_file(
    file_content: str, delimiter: Optional[str] = ","
) -> List[Dict[str, str]]:
    """
    Parses the content of a CSV file.

    Args:
        file_content (str): The content of the CSV file as a string.
        delimiter (str): The delimiter used in the CSV file.

    Returns:
        List[Dict[str, str]]: A list of dictionaries representing the parsed CSV data.

    Raises:
        ValueError: If there is an error parsing the CSV content.
    """
    import csv

    try:
        reader = csv.DictReader(StringIO(file_content), delimiter=delimiter)
        # next(reader, None)  # Skip the header row
        return [dict(row) for row in reader]
    except csv.Error as e:
        raise ValueError(f"Error: Could not parse CSV content. Details: {e}") from e


def get_schema_from_csv(
    file_path: str,
    table: str,
    delimiter: str = ",",
    query: str = None,
) -> List[Dict[str, Any]]:
    """
    Get schema from CSV schema_registry file

    Args:
        file_path: Path to the schema_registry CSV file
        table: Table name to look up in the registry
        delimiter: CSV delimiter (default: ',')
        query: Optional custom WHERE clause for additional filters (NOT SUPPORTED for CSV)

    Returns:
        List of dicts with schema information
    """
    import csv

    schema = []

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)

        for row in reader:
            if row.get("table_name") == table:
                schema.append(
                    {
                        "id": int(row["id"]) if row.get("id") else None,
                        "environment": row.get("environment"),
                        "source_type": row.get("source_type"),
                        "database_name": row.get("database_name"),
                        "catalog_name": row.get("catalog_name"),
                        "schema_name": row.get("schema_name"),
                        "table_name": row.get("table_name"),
                        "field": row.get("field"),
                        "data_type": row.get("data_type"),
                        "nullable": row.get("nullable", "").lower()
                        in ("true", "1", "yes"),
                        "max_length": (
                            int(row["max_length"])
                            if row.get("max_length") and row["max_length"].strip()
                            else None
                        ),
                        "comment": row.get("comment"),
                        "created_at": row.get("created_at"),
                        "updated_at": row.get("updated_at"),
                    }
                )

    if not schema:
        raise ValueError(f"No schema found in {file_path} " f"for table '{table}'")

    if query:
        import warnings

        warnings.warn(
            "The 'query' parameter is not supported for CSV sources and will be ignored"
        )

    return schema


def get_schema_from_s3(
    s3_path: str,
    table: str,
    delimiter: str = ",",
    query: str = None,
) -> List[Dict[str, Any]]:
    """
    Get schema from S3 schema_registry CSV file

    Args:
        s3_path: S3 URI to the schema_registry CSV file (e.g., 's3://bucket/path/schema_registry.csv')
        table: Table name to look up in the registry
        delimiter: CSV delimiter (default: ',')
        query: Optional custom WHERE clause for additional filters (NOT SUPPORTED for S3/CSV)

    Returns:
        List of dicts with schema information
    """
    import tempfile
    import os

    content = __read_s3_file(s3_path)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(content)
        temp_path = f.name

    try:
        return get_schema_from_csv(
            file_path=temp_path,
            table=table,
            delimiter=delimiter,
            query=query,
        )
    finally:
        os.unlink(temp_path)


def get_schema_from_mysql(
    host: str = None,
    user: str = None,
    password: str = None,
    database: str = None,
    table: str = None,
    port: int = 3306,
    registry_table: str = "schema_registry",
    query: str = None,
    conn=None,  # Novo parâmetro
) -> List[Dict[str, Any]]:
    """
    Get schema from MySQL schema_registry table

    Args:
        host: MySQL host (not needed if conn is provided)
        user: MySQL user (not needed if conn is provided)
        password: MySQL password (not needed if conn is provided)
        database: Database containing the registry table (not needed if conn is provided)
        table: Table name to look up in the registry
        port: MySQL port (default: 3306)
        registry_table: Name of the schema registry table (default: 'schema_registry')
        query: Optional custom WHERE clause for additional filters
        conn: Existing MySQL connection (optional, will create new if not provided)

    Returns:
        List of dicts with schema information
    """
    import mysql.connector

    if conn is None:
        if not all([host, user, password, database]):
            raise ValueError(
                "Either 'conn' or all of (host, user, password, database) must be provided"
            )
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database, port=port
        )
        should_close = True
    else:
        should_close = False

    cursor = conn.cursor(dictionary=True)

    base_where = "table_name = %s"
    params = [table]

    if query:
        where_clause = f"{base_where} AND ({query})"
    else:
        where_clause = base_where

    if not registry_table.isidentifier():
        raise ValueError(f"Invalid registry_table name: {registry_table}")

    cursor.execute(
        f"""
            SELECT 
                id,
                environment,
                source_type,
                database_name,
                catalog_name,
                schema_name,
                table_name,
                field,
                data_type,
                nullable,
                max_length,
                comment,
                created_at,
                updated_at
            FROM {registry_table}
            WHERE {where_clause}
            ORDER BY id
            """,
        params,
    )

    schema = cursor.fetchall()
    cursor.close()

    if should_close:
        conn.close()

    if not schema:
        raise ValueError(f"No schema found in {registry_table} for table '{table}'")

    return schema


def get_schema_from_postgresql(
    host: str = None,
    user: str = None,
    password: str = None,
    database: str = None,
    schema: str = None,
    table: str = None,
    port: int = 5432,
    registry_table: str = "schema_registry",
    query: str = None,
    conn=None,  # Novo parâmetro
) -> List[Dict[str, Any]]:
    """
    Get schema from PostgreSQL schema_registry table

    Args:
        host: PostgreSQL host (not needed if conn is provided)
        user: PostgreSQL user (not needed if conn is provided)
        password: PostgreSQL password (not needed if conn is provided)
        database: Database containing the registry table (not needed if conn is provided)
        schema: Schema containing the registry table
        table: Table name to look up in the registry
        port: PostgreSQL port (default: 5432)
        registry_table: Name of the schema registry table (default: 'schema_registry')
        query: Optional custom WHERE clause for additional filters
        conn: Existing PostgreSQL connection (optional, will create new if not provided)

    Returns:
        List of dicts with schema information
    """
    import psycopg2

    if conn is None:
        if not all([host, user, password, database]):
            raise ValueError(
                "Either 'conn' or all of (host, user, password, database) must be provided"
            )
        conn = psycopg2.connect(
            host=host, user=user, password=password, dbname=database, port=port
        )
        should_close = True
    else:
        should_close = False

    cursor = conn.cursor()

    base_where = "table_name = %s"
    params = [table]

    if query:
        where_clause = f"{base_where} AND ({query})"
    else:
        where_clause = base_where

    schema_name = schema or "public"

    if not schema_name.isidentifier():
        raise ValueError(f"Invalid schema name: {schema_name}")
    if not registry_table.isidentifier():
        raise ValueError(f"Invalid registry_table name: {registry_table}")

    cursor.execute(
        f"""
            SELECT
                id,
                environment,
                source_type,
                database_name,
                catalog_name,
                schema_name,
                table_name,
                field,
                data_type,
                nullable,
                max_length,
                comment,
                created_at,
                updated_at
            FROM {schema_name}.{registry_table}
            WHERE {where_clause}
            ORDER BY id
            """,
        params,
    )

    cols = cursor.fetchall()
    cursor.close()

    if should_close:
        conn.close()

    if not cols:
        raise ValueError(
            f"No schema found in {database}.{schema_name}.{registry_table} for table '{table}'"
        )

    return [
        {
            "id": row[0],
            "environment": row[1],
            "source_type": row[2],
            "database_name": row[3],
            "catalog_name": row[4],
            "schema_name": row[5],
            "table_name": row[6],
            "field": row[7],
            "data_type": row[8],
            "nullable": row[9],
            "max_length": row[10],
            "comment": row[11],
            "created_at": row[12],
            "updated_at": row[13],
        }
        for row in cols
    ]


def get_schema_from_bigquery(  # pylint: disable=too-many-arguments,too-many-locals
    project_id: str,
    dataset_id: str,
    table_id: str,
    credentials_path: str = None,
    registry_table: str = "schema_registry",
    query: str = None,
) -> List[Dict[str, Any]]:
    """
    Get schema from BigQuery schema_registry table

    Args:
        project_id: BigQuery project ID
        dataset_id: BigQuery dataset ID
        table_id: Table name to look up in the registry
        credentials_path: Path to service account credentials file
        registry_table: Name of the schema registry table
        query: Optional custom WHERE clause for additional filters

    Returns:
        List of dicts with schema information
    """
    from google.cloud import bigquery
    from google.oauth2 import service_account

    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        client = bigquery.Client(project=project_id, credentials=credentials)
    else:
        client = bigquery.Client(project=project_id)

    where_clause = "table_name = @table_name" + (f" AND ({query})" if query else "")

    sql = f"""
            SELECT 
                id,
                environment,
                source_type,
                database_name,
                catalog_name,
                schema_name,
                table_name,
                field,
                data_type,
                nullable,
                max_length,
                comment,
                created_at,
                updated_at
            FROM `{project_id}.{dataset_id}.{registry_table}`
            WHERE {where_clause}
            ORDER BY id
        """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table_id)
        ]
    )

    results = client.query(sql, job_config=job_config).result()

    schema = [
        {
            "id": row.id,
            "environment": row.environment,
            "source_type": row.source_type,
            "database_name": row.database_name,
            "catalog_name": row.catalog_name,
            "schema_name": row.schema_name,
            "table_name": row.table_name,
            "field": row.field,
            "data_type": row.data_type,
            "nullable": row.nullable,
            "max_length": row.max_length,
            "comment": row.comment,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
        for row in results
    ]

    if not schema:
        raise ValueError(
            f"No schema found in {project_id}.{dataset_id}.{registry_table} for table '{table_id}'"
        )

    return schema


def get_schema_from_glue(
    glue_context,
    database_name: str,
    table_name: str,
    registry_table: str = "schema_registry",
    query: str = None,
) -> List[Dict[str, Any]]:
    """
    Get schema from Glue Data Catalog schema_registry table

    Args:
        glue_context: GlueContext instance
        database_name: Glue database containing the registry table
        table_name: Table name to look up in the registry
        registry_table: Name of the schema registry table (default: 'schema_registry')
        query: Optional custom WHERE clause for additional filters

    Returns:
        List of dicts with schema information
    """
    from awsglue.context import GlueContext

    if not isinstance(glue_context, GlueContext):
        raise ValueError("Valid GlueContext required")

    spark = glue_context.spark_session

    table_name_escaped = __escape_sql_string(table_name)

    where_clause = f"table_name = '{table_name_escaped}'" + (
        f" AND ({query})" if query else ""
    )

    registry_df = spark.sql(
        f"""
        SELECT 
            id,
            environment,
            source_type,
            database_name,
            catalog_name,
            schema_name,
            table_name,
            field,
            data_type,
            nullable,
            max_length,
            comment,
            created_at,
            updated_at
        FROM {database_name}.{registry_table}
        WHERE {where_clause}
        ORDER BY id
    """
    )

    rows = registry_df.collect()

    if not rows:
        raise ValueError(
            f"No schema found in {database_name}.{registry_table} "
            f"for table '{table_name}'"
        )

    schema = []
    for row in rows:
        schema.append(
            {
                "id": row.id,
                "environment": row.environment,
                "source_type": row.source_type,
                "database_name": row.database_name,
                "catalog_name": row.catalog_name,
                "schema_name": row.schema_name,
                "table_name": row.table_name,
                "field": row.field,
                "data_type": row.data_type,
                "nullable": row.nullable,
                "max_length": row.max_length,
                "comment": row.comment,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
        )

    return schema


def get_schema_from_databricks(
    spark,
    catalog: str,
    schema: str,
    table: str,
    registry_table: str = "schema_registry",
    query: str = None,
) -> List[Dict[str, Any]]:
    """
    Get schema from Databricks Unity Catalog schema_registry table

    Args:
        spark: SparkSession instance
        catalog: Unity Catalog name containing the registry
        schema: Schema name containing the registry table
        table: Table name to look up in the registry
        registry_table: Name of the schema registry table (default: 'schema_registry')
        query: Optional custom WHERE clause for additional filters

    Returns:
        List of dicts with schema information
    """

    table_escaped = __escape_sql_string(table)

    base_where = f"table_name = '{table_escaped}'"

    if query:
        where_clause = f"{base_where} AND ({query})"
    else:
        where_clause = base_where

    registry_df = spark.sql(
        f"""
        SELECT 
            id,
            environment,
            source_type,
            database_name,
            catalog_name,
            schema_name,
            table_name,
            field,
            data_type,
            nullable,
            max_length,
            comment,
            created_at,
            updated_at
        FROM {catalog}.{schema}.{registry_table}
        WHERE {where_clause}
        ORDER BY id
    """
    )

    rows = registry_df.collect()

    if not rows:
        raise ValueError(
            f"No schema found in {catalog}.{schema}.{registry_table} "
            f"for table '{table}'"
        )

    result = []
    for row in rows:
        result.append(
            {
                "id": row.id,
                "environment": row.environment,
                "source_type": row.source_type,
                "database_name": row.database_name,
                "catalog_name": row.catalog_name,
                "schema_name": row.schema_name,
                "table_name": row.table_name,
                "field": row.field,
                "data_type": row.data_type,
                "nullable": row.nullable,
                "max_length": row.max_length,
                "comment": row.comment,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
        )

    return result


def get_schema_from_duckdb(
    conn,
    table: str,
    registry_table: str = "schema_registry",
    query: str = None,
) -> List[Dict[str, Any]]:
    """
    Get schema from DuckDB schema_registry table

    Args:
        conn: DuckDB connection object
        table: Table name to look up in the registry
        registry_table: Name of the schema registry table (default: 'schema_registry')
        query: Optional custom WHERE clause for additional filters

    Returns:
        List of dicts with schema information
    """

    table_escaped = __escape_sql_string(table)

    base_where = f"table_name = '{table_escaped}'"

    if query:
        where_clause = f"{base_where} AND ({query})"
    else:
        where_clause = base_where

    # Query no schema_registry
    df = conn.execute(
        f"""
        SELECT 
            id,
            environment,
            source_type,
            database_name,
            catalog_name,
            schema_name,
            table_name,
            field,
            data_type,
            nullable,
            max_length,
            comment,
            created_at,
            updated_at
        FROM {registry_table}
        WHERE {where_clause}
        ORDER BY id
    """
    ).fetchdf()

    if df.empty:
        raise ValueError(f"No schema found in {registry_table} " f"for table '{table}'")

    schema = []
    for _, row in df.iterrows():
        schema.append(
            {
                "id": row["id"],
                "environment": row["environment"],
                "source_type": row["source_type"],
                "database_name": row["database_name"],
                "catalog_name": row["catalog_name"],
                "schema_name": row["schema_name"],
                "table_name": row["table_name"],
                "field": row["field"],
                "data_type": row["data_type"],
                "nullable": row["nullable"],
                "max_length": row["max_length"],
                "comment": row["comment"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )

    return schema


def __parse_data(data: list[dict]) -> list[dict]:
    """
    Parse the configuration data with robust type handling.

    Args:
        data (List[Dict[str, Any]]): The raw data to be parsed.

    Returns:
        List[Dict[str, Any]]: A list of parsed configuration data.
    """
    parsed_data = []

    for row in data:
        try:
            field_value = row.get("field", "")
            if isinstance(field_value, str) and "[" in field_value:
                field_value = [
                    item.strip() for item in field_value.strip("[]").split(",")
                ]

            threshold_value = row.get("threshold")
            if threshold_value in [None, "NULL", ""]:
                threshold_value = 1.0
            else:
                try:
                    threshold_value = float(threshold_value)
                except (ValueError, TypeError):
                    threshold_value = 1.0

            updated_at_value = row.get("updated_at")
            if updated_at_value in [None, "NULL", ""]:
                updated_at_value = None
            elif isinstance(updated_at_value, (date, datetime)):
                updated_at_value = updated_at_value
            elif isinstance(updated_at_value, str):
                try:
                    updated_at_value = parser.parse(updated_at_value)
                except (ValueError, TypeError):
                    updated_at_value = None

            execute_value = row.get("execute", True)
            if isinstance(execute_value, str):
                execute_value = execute_value.lower() in ["true", "1", "yes", "y", "t"]
            else:
                execute_value = bool(execute_value)

            # Value
            value_field = row.get("value")
            if value_field in ["NULL", "", None]:
                value_field = None

            parsed_row = {
                "field": field_value,
                "check_type": row.get("check_type", ""),
                "value": value_field,
                "threshold": threshold_value,
                "execute": execute_value,
                "updated_at": updated_at_value,
            }
            parsed_data.append(parsed_row)

        except Exception as e:
            print(f"Warning: Error parsing row {row}. Error: {e}")
            continue

    return parsed_data


def __escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def __create_connection(connect_func, host, user, password, database, port) -> Any:
    """
    Helper function to create a database connection.

    Args:
        connect_func: A connection function (e.g., `mysql.connector.connect` or `psycopg2.connect`).
        host (str): The host of the database server.
        user (str): The username for the database.
        password (str): The password for the database.
        database (str): The name of the database.
        port (int): The port to connect to.

    Returns:
        Connection: A connection object for the database.

    Raises:
        ConnectionError: If there is an error establishing the connection.
    """
    try:
        return connect_func(
            host=host, user=user, password=password, database=database, port=port
        )
    except Exception as e:
        raise ConnectionError(f"Error creating connection: {e}")


def infer_basic_type(val: str) -> str:
    try:
        int(val)
        return "integer"
    except:
        pass
    try:
        float(val)
        return "float"
    except:
        pass
    try:
        _ = parser.parse(val)
        return "date"
    except:
        pass
    return "string"
