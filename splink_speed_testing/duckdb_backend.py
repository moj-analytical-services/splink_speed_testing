import duckdb


def create_table_from_parquet(
    con: duckdb.DuckDBPyConnection, path: str, table_name: str
) -> duckdb.DuckDBPyConnection:
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')")
    return con
