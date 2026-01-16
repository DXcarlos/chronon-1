import re
from typing import List

from sqlglot import exp, parse_one


def _strip_jinja_templates(query: str) -> str:
    """
    Replace Jinja-style template variables with placeholder values so sqlglot can parse the query.

    Handles patterns like:
    - {{ start_date }}
    - {{ end_date }}
    - {{ latest_date }}
    - {{ start_date(offset=-10, lower_bound='2023-01-01') }}
    - {{ max_date(table=namespace.my_table) }}
    """
    # Replace {{ ... }} patterns with a placeholder date string
    # Use unquoted value since templates are often already wrapped in quotes in the query
    return re.sub(r"\{\{[^}]*\}\}", "2000-01-01", query)


def tables_in_query(query: str, dialect: str = "spark") -> List[str]:
    """
    Get the tables in a query.
    Ex:
    >>> tables_in_query("SELECT * FROM sample_namespace.sample_table")
    ['sample_namespace.sample_table']
    """
    # Strip Jinja templates before parsing so sqlglot can handle the query
    clean_query = _strip_jinja_templates(query)
    parsed_query = parse_one(clean_query, dialect=dialect)
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