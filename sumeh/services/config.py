#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import StringIO
from dateutil import parser
from typing import List, Dict, Any, Tuple, Optional


def get_config_from_s3(s3_path: str, delimiter: str = ","):
    try:
        file_content = __read_s3_file(s3_path)
        data = __read_csv_file(file_content, delimiter)
        return __parse_data(data)

    except Exception as e:
        raise RuntimeError(f"Error reading or processing the S3 file: {e}")


def get_config_from_mysql(
    connection=None,
    host: str = None,
    user: str = None,
    password: str = None,
    database: str = None,
    port: int = 3306,
    schema: str = None,
    table: str = None,
    query: str = None,
):
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
    connection=None,
    host: str = None,
    user: str = None,
    password: str = None,
    database: str = None,
    port: int = 5432,
    schema: str = None,
    table: str = None,
    query: str = None,
) -> list[dict]:
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
) -> List[Dict[str, str]]:
    from google.cloud import bigquery
    from google.auth.exceptions import DefaultCredentialsError

    try:
        client = bigquery.Client(
            project=project_id,
            credentials=(
                None
                if credentials_path is None
                else bigquery.Credentials.from_service_account_file(credentials_path)
            ),
        )

        # Construct the SQL query
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"

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


def get_config_from_csv(file_path: str, delimiter: str = ",") -> List[Dict[str, str]]:
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


def __read_s3_file(s3_path: str) -> Optional[str]:
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
    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
            return file.read()
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Error: The file at '{file_path}' was not found."
        ) from e
    except IOError as e:
        raise IOError(f"Error: Could not read file '{file_path}'. Details: {e}") from e


def __read_csv_file(file_content: str, delimiter: str = ",") -> List[Dict[str, str]]:
    import csv

    try:
        reader = csv.DictReader(StringIO(file_content), delimiter=delimiter)
        next(reader, None)  # Skip the header row
        return [dict(row) for row in reader]
    except csv.Error as e:
        raise ValueError(f"Error: Could not parse CSV content. Details: {e}") from e


def __parse_data(data: list[dict]) -> list[dict]:
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
    return connect_func(
        host=host, user=user, password=password, database=database, port=port
    )
