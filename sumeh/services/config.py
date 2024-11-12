#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import StringIO
import pandas as pd
from dateutil import parser


def get_config_from_s3(s3_path: str, delimiter=","):
    file_content = __read_s3_file(s3_path)
    return __parse_data(__read_csv_file(file_content, delimiter))


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

    if query is None and (schema is None or table is None):
        raise ValueError("Você deve fornecer um 'query' ou 'schema' e 'table'.")

    if query is None:
        query = f"SELECT * FROM {schema}.{table}"

    connection = connection or __create_connection(
        mysql.connector.connect, host, user, password, database, port
    )

    data = pd.read_sql(query, connection)
    
    if host is not None:
        connection.close()
    data_dict = data.to_dict(orient='records')
    return __parse_data(data_dict)


def get_config_from_postgresql(
    host: str = None,
    user: str = None,
    password: str = None,
    database: str = None,
    port: int = 5432,
    schema: str = None,
    table: str = None,
    query: str = None,
):
    import psycopg2

    if query is None and (schema is None or table is None):
        raise ValueError("Você deve fornecer um 'query' ou 'schema' e 'table'.")

    if query is None:
        query = f"SELECT * FROM {schema}.{table}"

    connection = connection or __create_connection(
        psycopg2.connect, host, user, password, database, port
    )

    data = pd.read_sql(query, connection)
    if host is not None:
        connection.close()
    
    data_dict = data.to_dict(orient='records')
    return __parse_data(data_dict)


def get_config_from_bigquery(
    project_id: str, dataset_id: str, table_id: str, credentials_path: str = None
):
    from google.cloud import bigquery

    client = bigquery.Client(
        project=project_id,
        credentials=(
            None
            if credentials_path is None
            else bigquery.Credentials.from_service_account_file(credentials_path)
        ),
    )

    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    data = client.query(query).to_dataframe()
    data_dict = data.to_dict(orient='records')
    return __parse_data(data_dict)


def get_config_from_csv(file_path: str, delimiter: str=","):
    file_content = __read_local_file(file_path)
    result = __read_csv_file(file_content, delimiter)
    return __parse_data(result)


def __read_s3_file(s3_path: str):
    import boto3

    s3 = boto3.client("s3")
    bucket, key = __parse_s3_path(s3_path)
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


def __parse_s3_path(s3_path: str):
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    bucket, key = s3_path.split("/", 1)
    return bucket, key


def __read_local_file(file_path: str):
    with open(file_path, mode="r", encoding="utf-8") as file:
        return file.read()


def __read_csv_file(file_content: str, delimiter: str = ",") -> list[dict]:
    import csv
    reader = csv.DictReader(StringIO(file_content), delimiter=delimiter)
    next(reader, None)
    
    return [dict(row) for row in reader]


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

def __create_connection(connect_func, host, user, password, database, port):
    """Auxiliary function to create the connection."""
    return connect_func(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )
