import splink.comparison_level_library as cll
import duckdb
from splink_speed_testing.duckdb_backend import create_table_from_parquet


def setup_comparison_test(path):
    con = duckdb.connect()
    create_table_from_parquet(
        con=con,
        path=path,
        table_name="fullname_nonmatching",
    )
    sql_condition = (
        cll.ExactMatchLevel("full_name")
        .get_comparison_level("duckdb")
        .as_dict()["sql_condition"]
    )

    sql = f"""
    create table fullname_nonmatching_out as
    select {sql_condition} as c
    from fullname_nonmatching
    """

    return (con, sql), {}


def execute_comparison(con, sql):
    con.execute(sql)
    return con


def test_comparison_execution(benchmark, comparison_test_data_path):
    benchmark.pedantic(
        execute_comparison,
        setup=lambda: setup_comparison_test(comparison_test_data_path),
        rounds=2,
        iterations=1,
        warmup_rounds=0,
    )
