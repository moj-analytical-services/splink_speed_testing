from pathlib import Path


def create_table_from_parquet_duckdb(db_api, path: Path, table_name: str) -> str:
    if "cosine_similarity" in str(path):
        db_api._execute_sql_against_backend(
            f"""CREATE TABLE {table_name} AS
            SELECT
                vector_l::DOUBLE[10] as vector_l,
                vector_r::DOUBLE[10] as vector_r
            FROM read_parquet('{path}/*.parquet')"""
        )
    else:
        db_api._execute_sql_against_backend(
            f"""CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{path}/*.parquet')"""
        )
    return table_name


def create_table_from_parquet_spark(db_api, path: Path, table_name: str) -> str:
    spark = db_api.spark
    df = spark.read.parquet(str(path))
    df = df.repartition(db_api.num_partitions_on_repartition)
    df.persist()
    df.createOrReplaceTempView(table_name)
    return df


create_table_fns = {
    "duckdb": create_table_from_parquet_duckdb,
    "spark": create_table_from_parquet_spark,
}
