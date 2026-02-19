"""
PyFlink Engine - Streaming Data Quality Validation.

Apache Flink's Python API for real-time stream processing.
Validates events one-by-one, adding _dq_errors column.

⚠️  ROW-LEVEL ONLY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Streaming validation is fundamentally different from batch.
We validate rows individually - no aggregations, no uniqueness checks.

Supported Validations (20 rules):
    ✅ Completeness: is_complete, are_complete
    ✅ Comparison: is_equal, is_greater_than, is_less_than, is_positive, is_negative
    ✅ Membership: is_in, not_in
    ✅ Pattern: has_pattern, is_legit
    ✅ Date checks: (coming soon)

NOT Supported:
    ❌ Uniqueness (requires stateful aggregation)
    ❌ Table-level aggregations (requires windows)
    ❌ Cross-row validations

Key Concepts:
    - No .split() - returns ValidatedFlinkTable with .split() method
    - No .collect() - results are unbounded streams
    - Adds _dq_errors column (ARRAY of errors)
    - User decides: 1 sink (with markers) OR 2 sinks (bifurcation)

⚡ PERFORMANCE OPTIMIZATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For best performance, enable THREAD mode (PyFlink 1.15+):

    table_env.get_config().set("python.execution-mode", "thread")

THREAD mode runs Python UDFs inside JVM (10-100x faster than PROCESS mode).

Requirements:
    - PyFlink 1.15+
    - Python 3.8+
    - Simple UDFs only (Sumeh uses simple UDFs ✅)

⚠️  Fallback Warning:
    Flink may silently fallback to PROCESS mode if OTHER parts of your
    pipeline use unsupported features (UDAF, Pandas UDF, Async I/O, etc).

    Sumeh UDFs ARE THREAD-compatible. Check Flink logs for:
    "Falling back to PROCESS mode" warnings if performance is slow.

Performance Comparison:
    - THREAD mode: ~2-5x slower than Java (FAST)
    - PROCESS mode: ~10-100x slower than Java (SLOW)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Example - Basic Usage:
    >>> from pyflink.table import StreamTableEnvironment
    >>> from sumeh import pyflink as sumeh_pyflink
    >>>
    >>> # Create Flink environment (user configures)
    >>> t_env = StreamTableEnvironment.create(...)
    >>> t_env.get_config().set("python.execution-mode", "thread")  # Optional but recommended
    >>>
    >>> # Define source
    >>> t_env.execute_sql('''
    ...     CREATE TABLE users (
    ...         user_id INT,
    ...         email STRING,
    ...         age INT
    ...     ) WITH (
    ...         'connector' = 'kafka',
    ...         'topic' = 'users-input'
    ...     )
    ... ''')
    >>>
    >>> # Validate
    >>> result = sumeh_pyflink.validate(t_env, rules, "users")
    >>>
    >>> # Option A: Bifurcate (recommended)
    >>> good, bad = result.split()
    >>> good.execute_insert("clean_topic")
    >>> bad.execute_insert("dlq_topic")

Example - Single Sink:
    >>> # Keep all together (with error markers)
    >>> result = sumeh_pyflink.validate(t_env, rules, "users")
    >>> result.execute_insert("output_with_errors")

Example - Manual SQL Generation:
    >>> # Just generate SQL (no execution)
    >>> sql = sumeh_pyflink.get_validation_sql(rules, "users")
    >>> print(sql)
    >>>
    >>> # Register UDFs manually
    >>> sumeh_pyflink.register_udfs(t_env)
    >>>
    >>> # Execute SQL yourself
    >>> result = t_env.sql_query(sql)

Why PyFlink?
    - True streaming (event-by-event, not micro-batches)
    - Sub-second latency
    - Exactly-once semantics
    - Stateful processing (though we don't use for DQ)
    - Massive scale (Alibaba, Uber, Netflix)

Why NOT PyFlink?
    - Complex setup (JobManager + TaskManagers)
    - Python overhead (even with THREAD mode)
    - No table-level validations in streaming
    - Steep learning curve

Alternatives:
    - PySpark Structured Streaming (micro-batches, simpler)
    - Kafka Streams (Java/Scala only)
    - Ray Data (batch + streaming, Python-native)

Installation:
    pip install apache-flink

Sinks Supported:
    - Kafka
    - Iceberg
    - Hudi
    - Delta Lake
    - S3/HDFS (Parquet, ORC)
    - JDBC (Postgres, MySQL)
    - Elasticsearch

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from sumeh.engines.pyflink.engine import (
    register_udfs,
    validate,
)

__all__ = [
    "register_udfs",
    "validate",
]
