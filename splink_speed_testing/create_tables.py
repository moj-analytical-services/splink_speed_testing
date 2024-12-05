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

    count_sql = f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    print(f"\nTotal rows: {con.execute(count_sql).fetchone()[0]:,}")
    print(f"File size: {output_path.stat().st_size / (1024*1024):.2f} MB")

    con.close()

    return output_path


def create_comparison_test_table_dob_str_and_dob_date_most_nonmatching(
    num_output_rows: int = 1000,
    random_seed: int = 42,
    output_dir: Path = None,
) -> Path:
    con = duckdb.connect()
    df = splink_datasets.historical_50k

    input_size = int(num_output_rows**0.5)

    con.register("df", df)

    con.execute("SELECT setseed(0.42);")

    output_path = output_dir / "dob_str_and_dob_date_nonmatching.parquet"

    sql = f"""
    COPY (
        WITH df_limit AS (
            SELECT *
            FROM df
            ORDER BY random()
            LIMIT {input_size}
        )
        SELECT
            df_1.dob as dob_str_l,
            df_2.dob as dob_str_r,
            try_cast(df_1.dob as date) as dob_date_l,
            try_cast(df_2.dob as date) as dob_date_r
        FROM df_limit as df_1
        CROSS JOIN df_limit as df_2
        ORDER BY random()
    )
    TO '{output_path}'
    (FORMAT 'parquet')
    """

    con.execute(sql)

    count_sql = f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    print(f"\nTotal rows: {con.execute(count_sql).fetchone()[0]:,}")
    print(f"File size: {output_path.stat().st_size / (1024*1024):.2f} MB")

    con.close()

    return output_path


def create_comparison_test_table_postcode_arrays_most_nonmatching(
    num_output_rows: int = 1000,
    random_seed: int = 42,
    output_dir: Path = None,
) -> Path:
    con = duckdb.connect()
    df = splink_datasets.historical_50k

    input_size = int(num_output_rows**0.5)

    con.register("df", df)
    con.execute("SELECT setseed(0.42);")

    output_path = output_dir / "postcode_arrays_nonmatching.parquet"

    sql = f"""
    COPY (
        WITH df_arrays AS (
            SELECT
                cluster,
                list(DISTINCT postcode_fake)
                    FILTER (WHERE postcode_fake IS NOT NULL) AS postcode_array
            FROM df
            GROUP BY cluster
        ),
        df_with_arrays AS (
            SELECT
                df.*,
                df_arrays.postcode_array
            FROM df
            LEFT JOIN df_arrays USING (cluster)
            ORDER BY random()
            LIMIT {input_size}
        )
        SELECT
            df_1.postcode_array as postcode_array_l,
            df_2.postcode_array as postcode_array_r
        FROM df_with_arrays as df_1
        CROSS JOIN df_with_arrays as df_2
        ORDER BY random()
        LIMIT {num_output_rows}
    )
    TO '{output_path}'
    (FORMAT 'parquet')
    """

    con.execute(sql)

    # Print count and first 10 rows
    count_sql = f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    print(f"\nTotal rows: {con.execute(count_sql).fetchone()[0]:,}")
    print(f"File size: {output_path.stat().st_size / (1024*1024):.2f} MB")

    sample_sql = f"SELECT * FROM read_parquet('{output_path}') LIMIT 2"
    print("\nFirst 2 rows:")
    con.sql(sample_sql).show(max_width=10000)

    con.close()

    return output_path


def create_comparison_test_table_token_freq_arrays(
    num_output_rows: int = 1000,
    random_seed: int = 42,
    output_dir: Path = None,
) -> Path:
    con = duckdb.connect()
    df = splink_datasets.historical_50k

    # Make every 10th row first names john to increase size of intersection
    df.loc[::10, "first_name"] = "john"
    df.loc[1::10, "first_name"] = "william"
    df.loc[2::10, "first_name"] = "james"

    input_size = int(num_output_rows**0.5)

    con.register("df", df)
    con.execute("SELECT setseed(0.42);")

    output_path = output_dir / "token_freq_arrays_nonmatching.parquet"

    sql = f"""
    COPY (
        WITH
        -- Calculate relative frequency of each unique first name
        tokens AS (
            SELECT
                lower(first_name) as token,
                COUNT(*) * 1.0 / (SELECT COUNT(*) FROM df) as rel_freq
            FROM df
            GROUP BY token
        ),
        -- Get distinct first names for each cluster
        distinct_names_by_cluster AS (
            SELECT DISTINCT
                cluster,
                lower(first_name) as first_name
            FROM df
            WHERE first_name IS NOT NULL
        ),
        -- Create array of name/frequency pairs for each cluster
        df_arrays AS (
            SELECT
                cluster,
                list({{'value': first_name, 'rel_freq': rel_freq}}) AS token_freq_array
            FROM distinct_names_by_cluster
            LEFT JOIN tokens ON first_name = token
            GROUP BY cluster
        ),
        df_with_arrays AS (
            SELECT
                df.*,
                df_arrays.token_freq_array
            FROM df
            LEFT JOIN df_arrays USING (cluster)
            ORDER BY random()
            LIMIT {input_size}
        )
        SELECT
            df_1.token_freq_array as token_freq_array_l,
            df_2.token_freq_array as token_freq_array_r
        FROM df_with_arrays as df_1
        CROSS JOIN df_with_arrays as df_2
        ORDER BY random()
        LIMIT {num_output_rows}
    )
    TO '{output_path}'
    (FORMAT 'parquet')
    """

    con.execute(sql)

    # Print count and first 10 rows
    count_sql = f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    print(f"\nTotal rows: {con.execute(count_sql).fetchone()[0]:,}")
    print(f"File size: {output_path.stat().st_size / (1024*1024):.2f} MB")

    sample_sql = f"SELECT * FROM read_parquet('{output_path}') LIMIT 2"
    print("\nFirst 2 rows:")
    con.sql(sample_sql).show(max_width=10000)

    con.close()

    return output_path


def create_comparison_test_table_lat_lng_most_nonmatching(
    num_output_rows: int = 1000,
    random_seed: int = 42,
    output_dir: Path = None,
) -> Path:
    con = duckdb.connect()

    input_size = int(num_output_rows**0.5)

    con.execute("SELECT setseed(0.42);")

    output_path = output_dir / "lat_lng_nonmatching.parquet"

    sql = f"""
    COPY (
        WITH random_points AS (
            SELECT
                49.9 + random() * (58.7 - 49.9) as lat,
                -8.2 + random() * (1.8 - (-8.2)) as lng
            FROM range({input_size})
        )
        SELECT
            p1.lat as lat_l,
            p1.lng as lng_l,
            p2.lat as lat_r,
            p2.lng as lng_r
        FROM random_points as p1
        CROSS JOIN random_points as p2
        ORDER BY random()
        LIMIT {num_output_rows}
    )
    TO '{output_path}'
    (FORMAT 'parquet')
    """

    con.execute(sql)

    # Print count and first 2 rows
    count_sql = f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    print(f"\nTotal rows: {con.execute(count_sql).fetchone()[0]:,}")
    print(f"File size: {output_path.stat().st_size / (1024*1024):.2f} MB")

    sample_sql = f"SELECT * FROM read_parquet('{output_path}') LIMIT 2"
    print("\nFirst 2 rows:")
    con.sql(sample_sql).show(max_width=10000)

    con.close()

    return output_path


def create_comparison_test_table_cosine_similarity(
    num_output_rows: int = 1000,
    random_seed: int = 42,
    output_dir: Path = None,
) -> Path:
    con = duckdb.connect()

    input_size = int(num_output_rows**0.5)
    vector_length = 100
    con.execute("SELECT setseed(0.42);")

    output_path = output_dir / "cosine_similarity_nonmatching.parquet"

    sql = f"""
    COPY (
        WITH random_vectors AS (
            SELECT
                array_agg(CAST(random() AS FLOAT))::FLOAT[{vector_length}] AS vector
            FROM generate_series(1, {input_size}) AS group_number,
                generate_series(1, {vector_length})
            GROUP BY group_number
        )
        SELECT
            v1.vector::FLOAT[{vector_length}] as vector_l,
            v2.vector::FLOAT[{vector_length}] as vector_r
        FROM random_vectors as v1
        CROSS JOIN random_vectors as v2
        ORDER BY random()
        LIMIT {num_output_rows}
    )
    TO '{output_path}'
    (FORMAT 'parquet')
    """

    con.execute(sql)

    # Print count and first 2 rows
    count_sql = f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    print(f"\nTotal rows: {con.execute(count_sql).fetchone()[0]:,}")
    print(f"File size: {output_path.stat().st_size / (1024*1024):.2f} MB")

    sample_sql = f"SELECT * FROM read_parquet('{output_path}') LIMIT 2"
    print("\nFirst 2 rows:")
    con.sql(sample_sql).show(max_width=10000)

    con.close()

    return output_path
