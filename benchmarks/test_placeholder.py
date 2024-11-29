import pytest
import splink.comparison_level_library as cll
import duckdb
from splink_speed_testing.duckdb_backend import create_table_from_parquet


def execute_comparison(con, sql):
    con.sql(sql).show()


def fullname_nonmatching_con(comparison_test_data_path):
    con = duckdb.connect()
    create_table_from_parquet(
        con=con,
        path=comparison_test_data_path,
        table_name="fullname_nonmatching",
    )
    return con


@pytest.mark.parametrize(
    "comparison_level",
    [
        pytest.param(cll.ExactMatchLevel, id="Exact Match"),
        pytest.param(
            lambda col: cll.JaroLevel(col, distance_threshold=0.9), id="Jaro Level"
        ),
        pytest.param(
            lambda col: cll.JaroWinklerLevel(col, distance_threshold=0.9),
            id="Jaro-Winkler Level",
        ),
    ],
)
def test_comparison_execution(benchmark, comparison_level, comparison_test_data_path):
    def setup_comparison_test():
        con = fullname_nonmatching_con(comparison_test_data_path)

        sql_condition = (
            comparison_level("full_name")
            .get_comparison_level("duckdb")
            .as_dict()["sql_condition"]
        )

        sql = f"""
        select sum(cast({sql_condition} as int)) as c
        from fullname_nonmatching
        """

        return (con, sql), {}

    benchmark.pedantic(
        execute_comparison,
        setup=setup_comparison_test,
        rounds=2,
        iterations=1,
        warmup_rounds=0,
    )
