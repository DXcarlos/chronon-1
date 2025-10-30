from typing import List

from sqlglot import exp, parse_one


def tables_in_query(query: str, dialect: str = "spark") -> List[str]:
    """
    Get the tables in a query.
    Ex: 
    >>> tables_in_query("SELECT * FROM sample_namespace.sample_table")
    ['sample_namespace.sample_table']
    """
    parsed_query = parse_one(query, dialect=dialect)
    return [t.sql() for t in parsed_query.find_all(exp.Table)]


def normalize_table_name(table_name: str) -> str:
    """
    Normalize a table name. Standardize the names so we can compare them in the case of quotes and such.
    Ex: 
    >>> normalize_table_name("sample_namespace.sample_table")
    'sample_namespace.sample_table'
    >>> normalize_table_name("`sample_namespace.sample_table`")
    'sample_namespace.sample_table'
    >>> normalize_table_name('"sample_namespace.sample_table"')
    'sample_namespace.sample_table'
    """
    return table_name.replace("`", "").replace('"', "")