from pathlib import Path
from splink.datasets import splink_datasets
import duckdb


def create_comparison_test_table_full_name_most_nonmatching(
    num_output_rows: int = 1000,
    random_seed: int = 42,
    output_dir: Path = None,
) -> Path:
    con = duckdb.connect()
    df = splink_datasets.historical_50k

    input_size = int(num_output_rows**0.5)

    con.register("df", df)

    con.execute("SELECT setseed(0.42);")

    output_path = output_dir / "fullname_nonmatching.parquet"

    sql = f"""
    COPY (
        WITH df_limit AS (
            SELECT *
            FROM df
            ORDER BY random()
            LIMIT {input_size}
        )
        SELECT
            df_1.full_name as full_name_l,
            df_2.full_name as full_name_r
        FROM df_limit as df_1
        CROSS JOIN df_limit as df_2
        ORDER BY random()
    )
    TO '{output_path}'
    (FORMAT 'parquet')
    """

    con.execute(sql)
    con.close()

    return output_path
