# Sumeh - Quality Check and Config Management Tool

Sumeh is a tool designed to simplify the parameterization of the `cuallee` project. It provides an efficient way to retrieve configurations from various sources, such as `CSV files`, `S3`, `MySQL`, `PostgreSQL`, and `BigQuery`, and also offers features to validate these configurations based on defined rules.

## Purpose

The main goal of **Sumeh** is to streamline the collection of configurations for the **Cuallee** project, providing a configurator that can access and validate data from different sources, along with a series of quality checks to ensure the data meets the expected standards.

## Features

- **Configuration Collection**: Supports collecting configurations from multiple sources, including S3, MySQL, PostgreSQL, BigQuery, and CSV.
- **Data Validation**: Allows applying a series of validation rules to the collected data, including checks for completeness, uniqueness, or whether values meet a specific threshold.
- **Quality Check Facilitator**: Through integration with Cuallee, it is possible to perform checks like "is_complete," "is_unique," "is_greater_than," among others, to ensure data quality.

## Project Structure

The project structure is organized as follows:

```shell
.
├── README.md
├── poetry.lock
├── pyproject.toml
├── sumeh
│   ├── __init__.py
│   ├── __pycache__
│   │   ├── __init__.cpython-310.pyc
│   │   ├── __init__.cpython-312.pyc
│   │   ├── cli.cpython-312.pyc
│   │   └── sumeh.cpython-310.pyc
│   ├── cli.py
│   ├── services
│   │   ├── __init__.py
│   │   ├── __pycache__
│   │   │   ├── __init__.cpython-310.pyc
│   │   │   ├── config.cpython-310.pyc
│   │   │   ├── config_parser.cpython-310.pyc
│   │   │   ├── quality.cpython-310.pyc
│   │   │   └── utils.cpython-310.pyc
│   │   ├── config.py
│   │   ├── index.html
│   │   └── utils.py
│   └── sumeh.py
├── tags
└── tests
    ├── __init__.py
    ├── __pycache__
    │   ├── __init__.cpython-310.pyc
    │   └── test_sumeh.cpython-310-pytest-6.2.4.pyc
    ├── mock
    │   ├── config.csv
    │   └── data.csv
    └── test_sumeh.py
```


## Features

- **Configuration Retrieval**: Fetch configuration data from various sources:
  - Amazon S3
  - MySQL
  - PostgreSQL
  - BigQuery
  - CSV files

- **Quality Check**: Run quality checks on data using customizable rules:
  - Ensure completeness
  - Check uniqueness
  - Validate primary keys
  - Verify value ranges (greater than, less than, equal to)
  - And more!

- **Configurable Rules**: Define checks such as `is_complete`, `is_unique`, `is_primary_key`, and more, with flexible parameters like threshold values and value ranges.

## Installation

To get started with the project

```bash
pip install sumeh
```

## Usage

The main entry point is the sumeh.py script, which provides various utilities for working with configuration data and performing quality checks. You can also use the CLI for interacting with the configurator.


## List of avaliable checks:


| cover | check                          | description                                                                 |
|-----|------------------------------------|---------------------------------------------------------------------------------|
| [x] | **`is_complete`**                  | Checks if the field is complete with a specified threshold for completeness.    |
| [x] | **`is_unique`**                    | Checks if the field has unique values with a threshold for uniqueness.          |
| [x] | **`is_primary_key`**               | Ensures that the field is a primary key, with a threshold for the percentage of uniqueness. |
| [x] | **`are_complete`**                 | Verifies that multiple fields are complete, with a threshold for completeness.  |
| [x] | **`are_unique`**                   | Ensures that multiple fields have unique values, with a threshold for uniqueness. |
| [x] | **`is_composite_key`**             | Checks if a combination of fields forms a composite key, with a threshold for uniqueness. |
| [x] | **`is_greater_than`**              | Checks if the value of the field is greater than a specified value.             |
| [x] | **`is_positive`**                  | Checks if the field has only positive values.                                   |
| [x] | **`is_negative`**                  | Checks if the field has only negative values.                                   |
| [x] | **`is_greater_or_equal_than`**     | Checks if the value of the field is greater than or equal to a specified value. |
| [x] | **`is_less_than`**                 | Verifies if the value of the field is less than a specified value.              |
| [x] | **`is_less_or_equal_than`**        | Checks if the value of the field is less than or equal to a specified value.    |
| [x] | **`is_equal_than`**                | Verifies if the value of the field is equal to a specified value.               |
| [x] | **`is_contained_in`** (also `is_in`)| Checks if the field’s value is contained in a specified set of values.          |
| [x] | **`not_contained_in`** (also `not_in`) | Checks if the field’s value is not contained in a specified set of values.       |
| [x] | **`is_between`**                   | Checks if the field’s value is between two specified values.                    |
| [x] | **`has_pattern`**                  | Validates if the field’s value matches a specified pattern.                    |
| [x] | **`is_legit`**                     | Verifies if the field contains legitimate values according to a specific check. |
| [x] | **`has_min`**                      | Checks if the field’s value is greater than or equal to a specified minimum value. |
| [x] | **`has_max`**                      | Ensures that the field’s value is less than or equal to a specified maximum value. |
| [x] | **`has_std`**                      | Checks if the field has a specified standard deviation.                         |
| [x] | **`has_mean`**                     | Verifies if the field has a specified mean value.                               |
| [x] | **`has_sum`**                      | Ensures that the sum of the values in the field meets a specified value.        |
| [x] | **`has_cardinality`**              | Checks if the cardinality (distinct values) of the field matches a specified value. |
| [x] | **`has_infogain`**                 | Measures if the field has a certain information gain.                           |
| [x] | **`has_entropy`**                  | Verifies if the field has a specified entropy value.                            |
| [x] | **`is_in_millions`**               | Checks if the field’s values are within the millions range.                     |
| [x] | **`is_in_billions`**               | Verifies if the field’s values are in the billions range.                       |
| [x] | **`is_t_minus_1`**                 | Checks if the field’s value is equal to the date from one day ago.             |
| [x] | **`is_t_minus_2`**                 | Verifies if the field’s value is equal to the date from two days ago.           |
| [x] | **`is_t_minus_3`**                 | Checks if the field’s value is equal to the date from three days ago.          |
| [x] | **`is_today`**                     | Verifies if the field’s value is equal to today’s date.                         |
| [x] | **`is_yesterday`**                 | Checks if the field’s value is equal to yesterday’s date.                       |
| [x] | **`is_on_weekday`**                | Verifies if the field’s value corresponds to a weekday (Monday to Friday).     |
| [x] | **`is_on_weekend`**                | Checks if the field’s value corresponds to a weekend (Saturday or Sunday).     |
| [x] | **`is_on_monday`**                 | Verifies if the field’s value corresponds to Monday.                            |
| [x] | **`is_on_tuesday`**                | Checks if the field’s value corresponds to Tuesday.                            |
| [x] | **`is_on_wednesday`**              | Verifies if the field’s value corresponds to Wednesday.                         |
| [x] | **`is_on_thursday`**               | Checks if the field’s value corresponds to Thursday.                           |
| [x] | **`is_on_friday`**                 | Verifies if the field’s value corresponds to Friday.                           |
| [x] | **`is_on_saturday`**               | Checks if the field’s value corresponds to Saturday.                           |
| [x] | **`is_on_sunday`**                 | Verifies if the field’s value corresponds to Sunday.                           |
| [x] | **`satisfies`**                     | Ensures that the field satisfies a specified predicate (condition).            |


Each test corresponds to a specific check that can be applied to a dataset, using the rules defined in the code.

## Configuration Retrieval
You can retrieve configuration data using the following methods:

- #### From S3:
```python
from sumeh.services.config import get_config_from_s3
config = get_config_from_s3("s3://bucket/path/to/file.csv")
```

- #### From MySQL:
```python
from sumeh.services.config import get_config_from_mysql

config = get_config_from_mysql(
    host="localhost", user="root", password="password", database="test_db", table="config_table"
)
```
- #### From PostgreSQL:
```python
from sumeh.services.config import get_config_from_postgresql

config = get_config_from_postgresql(
    host="localhost", user="user", password="password", database="test_db", table="config_table"
)
```

- #### From BigQuery:
```python
from sumeh.services.config import get_config_from_bigquery

config = get_config_from_bigquery(
    project_id="your_project_id", dataset_id="your_dataset", table_id="your_table"
)
```
- #### From CSV:
```python
from sumeh.services.config import get_config_from_csv

config = get_config_from_csv("path/to/config.csv")
```

### Running Quality Checks
Once you have the configuration data, you can apply quality checks using the quality() function:

```python
from sumeh.sumeh import quality

rules = [
    {"check_type": "is_complete", "field": "column_name", "threshold": 0.9},
    {"check_type": "is_unique", "field": "id_column"}
]

df = quality(df, rules)
df.show()
```

### CLI Usage
You can also interact with the project through the command-line interface (CLI) provided by cli.py. To get started with the CLI, run:
```bash
sumeh-config
```

### Contributing

We welcome contributions to the Sumeh project! If you'd like to help improve the project, follow these steps:

Fork the repository.
Create a new branch for your changes.
Make your changes and ensure the tests pass.
Submit a pull request.

### License

This project is licensed under the MIT License.

### Acknowledgements

The Cuallee project is a key dependency for this project, providing the necessary tools for `quality` checks.
Thanks to the open-source libraries used in this project:
- `pandas`
- `dateutil`
- `mysql-connector-python`
- `psycopg2`
- `google-cloud-bigquery`
- `boto3`
- `poetry`

Let me know if you need further adjustments or clarifications!