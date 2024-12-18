import pytest
import splink.comparison_level_library as cll

from benchmarks.decorator import mark_with_dialects_excluding
from benchmarks.backends import create_table_fns
from splink_speed_testing.raw_sql_comparisons import (
    duckdb_pairwise_array_string_similarity,
    spark_pairwise_array_string_similarity,
    duckdb_tf_product_array,
)


def execute_comparison(db_api, sql):
    # t = db_api._sql_to_splink_dataframe(sql, "templated_name", "physical_name")
    t = db_api._execute_sql_against_backend(sql)

    # Trigger an action to ensure the query must be executed
    if db_api.sql_dialect.sql_dialect_str == "spark":
        t.toPandas()
    if db_api.sql_dialect.sql_dialect_str == "duckdb":
        t.df()


@mark_with_dialects_excluding("sqlite")
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
def test_comparison_execution_str_cll(
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


@mark_with_dialects_excluding("sqlite")
@pytest.mark.parametrize(
    "comparison_level,col_name",
    [
        pytest.param(
            lambda col: cll.AbsoluteDateDifferenceLevel(
                col, input_is_string=True, metric="day", threshold=5
            ),
            "dob_str",
            id="Absolute Date Difference Level (string)",
        ),
        pytest.param(
            lambda col: cll.AbsoluteDateDifferenceLevel(
                col, input_is_string=False, metric="day", threshold=5
            ),
            "dob_date",
            id="Absolute Date Difference Level (date)",
        ),
    ],
)
def test_comparison_execution_date_cll(
    test_helpers,
    dialect,
    benchmark,
    comparison_level,
    col_name,
    parquet_path_dob_str_and_dob_date_nonmatching,
):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    create_table_fn = create_table_fns[dialect]
    create_table_fn(
        db_api,
        parquet_path_dob_str_and_dob_date_nonmatching,
        "dob_str_and_dob_date_nonmatching",
    )

    def setup_comparison_test():
        sql_condition = (
            comparison_level(col_name)
            .get_comparison_level(dialect)
            .as_dict()["sql_condition"]
        )

        sql = f"""
        select sum(cast({sql_condition} as int)) as c
        from dob_str_and_dob_date_nonmatching
        """

        return (db_api, sql), {}

    benchmark.pedantic(
        execute_comparison,
        setup=setup_comparison_test,
        rounds=5,
        iterations=1,
        warmup_rounds=0,
    )


@mark_with_dialects_excluding("sqlite")
@pytest.mark.parametrize(
    "comparison_level",
    [
        pytest.param(
            lambda col: cll.ArrayIntersectLevel(col, min_intersection=1),
            id="Array Intersect Level",
        ),
        pytest.param(lambda col: cll.ArraySubsetLevel(col), id="Array Subset Level"),
    ],
)
def test_comparison_execution_array(
    test_helpers,
    dialect,
    benchmark,
    comparison_level,
    parquet_path_postcode_arrays_nonmatching,
):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    create_table_fn = create_table_fns[dialect]
    create_table_fn(
        db_api,
        parquet_path_postcode_arrays_nonmatching,
        "postcode_arrays_nonmatching",
    )

    def setup_comparison_test():
        sql_condition = (
            comparison_level("postcode_array")
            .get_comparison_level(dialect)
            .as_dict()["sql_condition"]
        )

        sql = f"""
        select sum(cast({sql_condition} as int)) as c
        from postcode_arrays_nonmatching
        """

        return (db_api, sql), {}

    benchmark.pedantic(
        execute_comparison,
        setup=setup_comparison_test,
        rounds=5,
        iterations=1,
        warmup_rounds=0,
    )


@mark_with_dialects_excluding("sqlite", "spark")
@pytest.mark.parametrize(
    "raw_sql_condition",
    [
        pytest.param(
            {
                "duckdb": duckdb_pairwise_array_string_similarity.format(
                    col_name="postcode_array",
                    threshold=2,
                    similarity_function="levenshtein",
                ),
                "spark": spark_pairwise_array_string_similarity.format(
                    col_name="postcode_array",
                    threshold=2,
                    similarity_function="levenshtein",
                ),
            },
            id="SQL Levenshtein for all pairs in array",
        ),
    ],
)
def test_comparison_execution_raw_sql(
    test_helpers,
    dialect,
    benchmark,
    raw_sql_condition,
    parquet_path_postcode_arrays_nonmatching,
):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    create_table_fn = create_table_fns[dialect]
    create_table_fn(
        db_api, parquet_path_postcode_arrays_nonmatching, "postcode_arrays_nonmatching"
    )

    def setup_comparison_test():
        sql_condition = raw_sql_condition[dialect]

        sql = f"""
        select sum(cast({sql_condition} as int)) as c
        from postcode_arrays_nonmatching
        """

        return (db_api, sql), {}

    benchmark.pedantic(
        execute_comparison,
        setup=setup_comparison_test,
        rounds=5,
        iterations=1,
        warmup_rounds=0,
    )


@mark_with_dialects_excluding("sqlite", "spark")
@pytest.mark.parametrize(
    "raw_sql_condition",
    [
        pytest.param(
            {
                "duckdb": duckdb_tf_product_array.format(
                    token_rel_freq_array_name="token_freq_array", threshold=0.0001
                ),
            },
            id="Raw SQL Token Frequency Product",
        ),
    ],
)
def test_comparison_execution_tf_product(
    test_helpers,
    dialect,
    benchmark,
    raw_sql_condition,
    parquet_path_token_freq_arrays_nonmatching,
):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    create_table_fn = create_table_fns[dialect]
    create_table_fn(
        db_api,
        parquet_path_token_freq_arrays_nonmatching,
        "token_freq_arrays_nonmatching",
    )

    def setup_comparison_test():
        sql_condition = raw_sql_condition[dialect]

        sql = f"""
        select sum(cast({sql_condition} as int)) as c
        from token_freq_arrays_nonmatching
        """

        return (db_api, sql), {}

    benchmark.pedantic(
        execute_comparison,
        setup=setup_comparison_test,
        rounds=5,
        iterations=1,
        warmup_rounds=0,
    )


@mark_with_dialects_excluding("sqlite")
@pytest.mark.parametrize(
    "comparison_level",
    [
        pytest.param(
            lambda lat_col, long_col: cll.DistanceInKMLevel(
                lat_col=lat_col, long_col=long_col, km_threshold=5
            ),
            id="Distance In KM Level",
        ),
    ],
)
def test_comparison_execution_distance_in_km(
    test_helpers,
    dialect,
    benchmark,
    comparison_level,
    parquet_path_lat_lng_nonmatching,
):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    create_table_fn = create_table_fns[dialect]
    create_table_fn(db_api, parquet_path_lat_lng_nonmatching, "lat_lng_nonmatching")

    def setup_comparison_test():
        sql_condition = (
            comparison_level("lat", "lng")
            .get_comparison_level(dialect)
            .as_dict()["sql_condition"]
        )

        sql = f"""
        select sum(cast({sql_condition} as int)) as c
        from lat_lng_nonmatching
        """

        return (db_api, sql), {}

    benchmark.pedantic(
        execute_comparison,
        setup=setup_comparison_test,
        rounds=5,
        iterations=1,
        warmup_rounds=0,
    )


@mark_with_dialects_excluding("sqlite", "spark")
@pytest.mark.parametrize(
    "comparison_level",
    [
        pytest.param(
            lambda col: cll.CosineSimilarityLevel(col, similarity_threshold=0.8),
            id="Cosine Similarity Level",
        ),
    ],
)
def test_comparison_execution_cosine_similarity(
    test_helpers,
    dialect,
    benchmark,
    comparison_level,
    parquet_path_cosine_similarity_nonmatching,
):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    create_table_fn = create_table_fns[dialect]
    create_table_fn(
        db_api,
        parquet_path_cosine_similarity_nonmatching,
        "cosine_similarity_nonmatching_array_size_10",
    )

    # Cosine similarity level requires fixed-size arrays in duckdb
    # Parquet format’s lack of inherent support for fixed-size arrays, leading DuckDB
    # to interpret these columns as variable-length lists (DOUBLE[]) when reading from
    # Parquet files.

    def setup_comparison_test():
        sql_condition = (
            comparison_level("vector")
            .get_comparison_level(dialect)
            .as_dict()["sql_condition"]
        )

        sql = f"""
        select sum(cast({sql_condition} as int)) as c
        from cosine_similarity_nonmatching_array_size_10
        """

        return (db_api, sql), {}

    benchmark.pedantic(
        execute_comparison,
        setup=setup_comparison_test,
        rounds=5,
        iterations=1,
        warmup_rounds=0,
    )
