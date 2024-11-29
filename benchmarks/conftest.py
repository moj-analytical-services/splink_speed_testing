import tempfile
from pathlib import Path
import pytest

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
