from splink.datasets import splink_datasets
import splink.comparison_level_library as cll
import duckdb


def create_comparison_test_table_full_name_most_nonmatching(
    connection: duckdb.DuckDBPyConnection,
    final_size: int = 1000,
    random_seed: int = 42,
    output_dir: str = None,
) -> None:
    df = splink_datasets.historical_50k

    # Calculate input size needed to achieve desired final size after cross join
    input_size = int(final_size**0.5)

    connection.register("df", df)

    sql = f"""
    COPY (
        WITH df_limit AS (
            SELECT *
            FROM df
            ORDER BY random({random_seed})
            LIMIT {input_size}
        )
        SELECT
            df_1.full_name as left_full_name,
            df_2.full_name as right_full_name
        FROM df_limit as df_1
        CROSS JOIN df as df_2
        ORDER BY random({random_seed})
    )
    TO '{output_dir}/comparison_test.parquet'
    (FORMAT 'parquet')
    """

    connection.execute(sql)
