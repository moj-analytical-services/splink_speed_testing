import pytest
import splink.comparison_level_library as cll
import duckdb

from benchmarks.decorator import mark_with_dialects_excluding
from benchmarks.backends import create_table_fns


def execute_comparison(db_api, sql):
    # t = db_api._sql_to_splink_dataframe(sql, "templated_name", "physical_name")
    t = db_api._execute_sql_against_backend(sql)

    # Trigger an action to ensure the query must be executed
    if db_api.sql_dialect.sql_dialect_str == "spark":
        print(t.toPandas())
    if db_api.sql_dialect.sql_dialect_str == "duckdb":
        print(t.df())


@mark_with_dialects_excluding("sqlite", "spark")
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
        pytest.param(
            lambda col: cll.DamerauLevenshteinLevel(col, distance_threshold=1),
            id="Damerau-Levenshtein Level",
        ),
        pytest.param(
            lambda col: cll.LevenshteinLevel(col, distance_threshold=1),
            id="Levenshtein Level",
        ),
        pytest.param(
            lambda col: cll.JaccardLevel(col, distance_threshold=0.9),
            id="Jaccard Level",
        ),
    ],
)
def test_comparison_execution(
    test_helpers,
    dialect,
    benchmark,
    comparison_level,
    parquet_path_fullname_nonmatching,
):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    create_table_fn = create_table_fns[dialect]
    create_table_fn(db_api, parquet_path_fullname_nonmatching, "fullname_nonmatching")

    def setup_comparison_test():
        sql_condition = (
            comparison_level("full_name")
            .get_comparison_level(dialect)
            .as_dict()["sql_condition"]
        )

        sql = f"""
        select sum(cast({sql_condition} as int)) as c
        from fullname_nonmatching
        """

        return (db_api, sql), {}

    benchmark.pedantic(
        execute_comparison,
        setup=setup_comparison_test,
        rounds=5,
        iterations=1,
        warmup_rounds=0,
    )
