"""
SQL transpiler using SQLGlot.

Converts SQL between different dialects.
"""

import sqlglot


def transpile(sql: str, from_dialect: str, to_dialect: str) -> str:
    """
    Transpile SQL from one dialect to another.

    Args:
        sql: SQL statement to transpile
        from_dialect: Source dialect (e.g. 'mysql')
        to_dialect: Target dialect (e.g. 'postgres')

    Returns:
        Transpiled SQL statement

    Example:
        >>> sql = "SELECT * FROM users LIMIT 10"
        >>> transpile(sql, "mysql", "mssql")
        'SELECT * FROM users OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY'
    """
    parsed = sqlglot.parse_one(sql, read=from_dialect)
    return parsed.sql(dialect=to_dialect, pretty=True)
