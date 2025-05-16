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
        Infers the basic data type of a given value.
"""
from io import StringIO
from dateutil import parser
from typing import List, Dict, Any, Tuple, Optional


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
    connection: Optional = None,
    host: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    port: Optional[int] = 3306,
    schema: Optional[str] = None,
    table: Optional[str] = None,
    query: Optional[str] = None,
):
    """
    Retrieves configuration data from a MySQL database.

    Args:
        connection (Optional): An existing MySQL connection object.
        host (Optional[str]): Host of the MySQL server.
        user (Optional[str]): Username to connect to MySQL.
        password (Optional[str]): Password for the MySQL user.
        database (Optional[str]): Database name to query.
        port (Optional[int]): The port for the MySQL connection (default is 3306).
        schema (Optional[str]): Schema name if query is not provided.
        table (Optional[str]): Table name if query is not provided.
        query (Optional[str]): Custom SQL query to fetch data (if not provided, `schema` and `table` must be given).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the parsed configuration data.

    Raises:
        ValueError: If neither `query` nor both `schema` and `table` are provided.
        ConnectionError: If there is an error connecting to MySQL.
        RuntimeError: If there is an error executing the query or processing the data.
    """
    import mysql.connector
    import pandas as pd

    if query is None and (schema is None or table is None):
        raise ValueError(
            "You must provide either a 'query' or both 'schema' and 'table'."
        )

    if query is None:
        query = f"SELECT * FROM {schema}.{table}"

    try:
        connection = connection or __create_connection(
            mysql.connector.connect, host, user, password, database, port
        )
        data = pd.read_sql(query, connection)
        data_dict = data.to_dict(orient="records")
        return __parse_data(data_dict)

    except mysql.connector.Error as e:
        raise ConnectionError(f"Error connecting to MySQL database: {e}")

    except Exception as e:
        raise RuntimeError(f"Error executing the query or processing data: {e}")

    finally:
        if connection and host is not None:
            connection.close()


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
    from google.auth.exceptions import DefaultCredentialsError

    if query is None:
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"

    try:
        client = bigquery.Client(
            project=project_id,
            credentials=(
                None
                if credentials_path is None
                else bigquery.Credentials.from_service_account_file(credentials_path)
            ),
        )

        # Execute the query and convert the result to a pandas DataFrame
        data = client.query(query).to_dataframe()

        # Convert the DataFrame to a list of dictionaries
        data_dict = data.to_dict(orient="records")

        # Parse the data and return the result
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
        dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
            database=database_name, table_name=table_name
        )

        data_frame = dynamic_frame.toDF()

        if query:
            data_frame.createOrReplaceTempView("table_name")
            data_frame = spark.sql(query)

        data_dict = [row.asDict() for row in data_frame.collect()]

        return __parse_data(data_dict)

    except Exception as e:
        raise RuntimeError(
            f"Error occurred while querying Glue Data Catalog: {e}"
        ) from e


def get_config_from_duckdb(
    db_path: str, table: str = None, query: str = None, conn=None
) -> List[Dict[str, Any]]:
    """
    Retrieve configuration data from a DuckDB database.

    This function fetches data from a DuckDB database either by executing a custom SQL query
    or by selecting all rows from a specified table. The data is then parsed into a list of
    dictionaries.

    Args:
        db_path (str): The path to the DuckDB database file.
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
    catalog: Optional[str], schema: Optional[str], table: str, **kwargs
) -> List[Dict[str, Any]]:
    """
    Retrieves configuration data from a Databricks table and returns it as a list of dictionaries.

    Args:
        catalog (Optional[str]): The catalog name in Databricks. If provided, it will be included in the table's full path.
        schema (Optional[str]): The schema name in Databricks. If provided, it will be included in the table's full path.
        table (str): The name of the table to retrieve data from.
        **kwargs: Additional keyword arguments (currently unused).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents a row of data from the table.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
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
    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
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
        content (str): The content of the CSV file as a string.
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
    file_path: str, delimiter: str = ",", sample_size: int = 1_000
) -> List[Dict[str, Any]]:
    import csv

    cols: Dict[str, Dict[str, Any]] = {}
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for idx, row in enumerate(reader):
            if idx >= sample_size:
                break
            for name, raw in row.items():
                info = cols.setdefault(
                    name,
                    {
                        "field": name,
                        "nullable": False,
                        "max_length": 0,
                        "type_hints": set(),
                    },
                )
                if raw == "" or raw is None:
                    info["nullable"] = True
                    continue
                info["max_length"] = max(info["max_length"], len(raw))
                info["type_hints"].add(infer_basic_type(raw))

    out: List[Dict[str, Any]] = []
    for info in cols.values():
        hints = info["type_hints"]
        if hints == {"integer"}:
            dtype = "integer"
        elif hints <= {"integer", "float"}:
            dtype = "float"
        elif hints == {"date"}:
            dtype = "date"
        else:
            dtype = "string"
        out.append(
            {
                "field": info["field"],
                "data_type": dtype,
                "nullable": info["nullable"],
                "max_length": info["max_length"] or None,
            }
        )
    return out


def get_schema_from_s3(s3_path: str, **kwargs) -> List[Dict[str, Any]]:

    content = __read_s3_file(s3_path)
    with open("/tmp/temp.csv", "w") as f:
        f.write(content)
    return get_schema_from_csv("/tmp/temp.csv", **kwargs)


def get_schema_from_mysql(
    host: str, user: str, password: str, database: str, table: str, port: int = 3306
) -> List[Dict[str, Any]]:
    import mysql.connector

    conn = mysql.connector.connect(
        host=host, user=user, password=password, database=database, port=port
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        f"""
        SELECT 
            column_name AS field,
            data_type,
            is_nullable = 'YES' AS nullable,
            character_maximum_length AS max_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """,
        (database, table),
    )
    schema = cursor.fetchall()
    cursor.close()
    conn.close()
    return schema


def get_schema_from_postgresql(
    host: str, user: str, password: str, database: str, table: str, port: int = 5432
) -> List[Dict[str, Any]]:
    import psycopg2

    conn = psycopg2.connect(
        host=host, user=user, password=password, dbname=database, port=port
    )
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT
            column_name AS field,
            data_type,
            is_nullable = 'YES' AS nullable,
            character_maximum_length AS max_length
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
    """,
        (table,),
    )
    cols = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {"field": f, "data_type": dt, "nullable": nl, "max_length": ml}
        for f, dt, nl, ml in cols
    ]


def get_schema_from_bigquery(
    project_id: str, dataset_id: str, table_id: str, credentials_path: str = None
) -> List[Dict[str, Any]]:
    from google.cloud import bigquery

    client = bigquery.Client(
        project=project_id,
        credentials=(
            None
            if credentials_path is None
            else bigquery.Credentials.from_service_account_file(credentials_path)
        ),
    )
    table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
    return [
        {
            "field": schema_field.name,
            "data_type": schema_field.field_type.lower(),
            "nullable": schema_field.is_nullable,
            "max_length": None,
        }
        for schema_field in table.schema
    ]


def get_schema_from_glue(
    glue_context, database_name: str, table_name: str
) -> List[Dict[str, Any]]:
    from awsglue.context import GlueContext

    if not isinstance(glue_context, GlueContext):
        raise ValueError("Informe um GlueContext válido")
    df = glue_context.spark_session.read.table(f"{database_name}.{table_name}")
    return [
        {
            "field": field.name,
            "data_type": field.dataType.simpleString(),
            "nullable": field.nullable,
            "max_length": None,
        }
        for field in df.schema.fields
    ]


def get_schema_from_duckdb(db_path: str, table: str, conn) -> List[Dict[str, Any]]:
    df = conn.execute(f"PRAGMA table_info('{table}')").fetchdf()
    return [
        {
            "field": row["name"],
            "data_type": row["type"].lower(),
            "nullable": not bool(row["notnull"]),
            "max_length": None,
        }
        for _, row in df.iterrows()
    ]


def get_schema_from_databricks(
    catalog: Optional[str], schema: Optional[str], table: str, **kwargs
) -> List[Dict[str, Any]]:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    if catalog and schema:
        full = f"{catalog}.{schema}.{table}"
    elif schema:
        full = f"{schema}.{table}"
    else:
        full = table
    schema = spark.table(full).schema
    result = []
    for f in schema.fields:
        result.append(
            {
                "field": f.name,
                "data_type": f.dataType.simpleString(),
                "nullable": f.nullable,
                "max_length": None,
            }
        )
    return result


def __parse_data(data: list[dict]) -> list[dict]:
    """
    Parse the configuration data.

    Args:
        data (List[Dict[str, str]]): The raw data to be parsed.

    Returns:
        List[Dict[str, str]]: A list of parsed configuration data.
    """
    parsed_data = []

    for row in data:
        parsed_row = {
            "field": (
                row["field"].strip("[]").split(",")
                if "[" in row["field"]
                else row["field"]
            ),
            "check_type": row["check_type"],
            "value": None if row["value"] == "NULL" else row["value"],
            "threshold": (
                None if row["threshold"] == "NULL" else float(row["threshold"])
            ),
            "execute": (
                row["execute"].lower() == "true"
                if isinstance(row["execute"], str)
                else row["execute"] is True
            ),
            "updated_at": parser.parse(row["updated_at"]),
        }
        parsed_data.append(parsed_row)

    return parsed_data


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
