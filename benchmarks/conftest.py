import tempfile
from pathlib import Path
import pytest
import duckdb
from splink_speed_testing.duckdb_backend import create_table_from_parquet

from splink_speed_testing.create_tables import (
    create_comparison_test_table_full_name_most_nonmatching,
)


@pytest.fixture(scope="session")
def comparison_test_data_path():
    temp_dir = Path(tempfile.mkdtemp(dir="./temp_data"))
    output_path = create_comparison_test_table_full_name_most_nonmatching(
        output_dir=temp_dir,
        num_output_rows=1e6,
    )

    yield output_path


@pytest.fixture(scope="session")
def fullname_nonmatching_con(comparison_test_data_path):
    con = duckdb.connect()
    create_table_from_parquet(
        con=con,
        path=comparison_test_data_path,
        table_name="fullname_nonmatching",
    )
    yield con
