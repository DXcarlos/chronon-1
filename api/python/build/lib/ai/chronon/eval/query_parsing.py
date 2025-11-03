from typing import List


def get_tables_from_query(sql_query) -> List[str]:
    import sqlglot

    # Parse the query
    parsed = sqlglot.parse_one(sql_query, dialect="bigquery")

    # Extract all table references
    tables = parsed.find_all(sqlglot.exp.Table)

    table_names = []
    for table in tables:
        name_parts = [part for part in [table.catalog, table.db, table.name] if part]
        table_name = ".".join(name_parts)
        table_names.append(table_name)

    return table_names
