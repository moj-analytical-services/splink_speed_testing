# https://github.com/moj-analytical-services/splink/blob/master/tests/helpers.py
import sqlite3
from abc import ABC, abstractmethod
from collections import UserDict

import pandas as pd

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker
from splink.internals.spark.database_api import SparkAPI
from splink.internals.sqlite.database_api import SQLiteAPI


class TestHelper(ABC):
    @property
    def Linker(self) -> Linker:
        return Linker

    @property
    @abstractmethod
    def DatabaseAPI(self):
        pass

    def db_api_args(self):
        return {}

    def extra_linker_args(self):
        # create fresh api each time
        return {"db_api": self.DatabaseAPI(**self.db_api_args())}

    @property
    def date_format(self):
        return "yyyy-mm-dd"

    @abstractmethod
    def convert_frame(self, df):
        pass

    def load_frame_from_csv(self, path):
        return pd.read_csv(path)

    def load_frame_from_parquet(self, path):
        return pd.read_parquet(path)

    @property
    def arrays_from(self) -> int:
        return 1


class DuckDBTestHelper(TestHelper):
    @property
    def DatabaseAPI(self):
        return DuckDBAPI

    def convert_frame(self, df):
        return df

    @property
    def date_format(self):
        return "%Y-%m-%d"


class SparkTestHelper(TestHelper):
    def __init__(self, spark_creator_function):
        self.spark = spark_creator_function()

    @property
    def DatabaseAPI(self):
        return SparkAPI

    def db_api_args(self):
        return {
            "spark_session": self.spark,
            "num_partitions_on_repartition": 2,
            "break_lineage_method": "parquet",
        }

    def convert_frame(self, df):
        spark_frame = self.spark.createDataFrame(df)
        spark_frame.persist()
        return spark_frame

    def load_frame_from_csv(self, path):
        df = self.spark.read.csv(path, header=True)
        df.persist()
        return df

    def load_frame_from_parquet(self, path):
        df = self.spark.read.parquet(path)
        df.persist()
        return df

    @property
    def arrays_from(self) -> int:
        return 0


class SQLiteTestHelper(TestHelper):
    _frame_counter = 0

    def __init__(self):
        self.con = sqlite3.connect(":memory:")
        self._frame_counter = 0

    def db_api_args(self):
        return {"connection": self.con}

    @property
    def DatabaseAPI(self):
        return SQLiteAPI

    @classmethod
    def _get_input_name(cls):
        name = f"input_alias_{cls._frame_counter}"
        cls._frame_counter += 1
        return name

    def convert_frame(self, df):
        name = self._get_input_name()
        df.to_sql(name, self.con, if_exists="replace")
        return name

    def load_frame_from_csv(self, path):
        return self.convert_frame(super().load_frame_from_csv(path))

    def load_frame_from_parquet(self, path):
        return self.convert_frame(super().load_frame_from_parquet(path))


class SplinkTestException(Exception):
    pass


class LazyDict(UserDict):
    """
    LazyDict
    Like a dict, but values passed are tuples of the form (func, args)
    getting returns the result of the function
    only instantiate the result when we need it.
    Need this for handling test fixtures.
    Disallow setting/deleting entries to catch errors -
    should be effectively immutable
    """

    # write only in creation
    def __init__(self, **kwargs):
        self.data = {}
        # set of keys we have accessed
        self.accessed = set()
        for key, val in kwargs.items():
            self.data[key] = val

    def __getitem__(self, key):
        func, args = self.data[key]
        self.accessed.add(key)
        return func(*args)

    def __setitem__(self, key, value):
        raise SplinkTestException(
            "LazyDict does not support setting values. "
            "Did you mean to read value instead?"
        )

    def __delitem__(self, key):
        raise SplinkTestException(
            "LazyDict does not support deleting items. "
            "Did you mean to read value instead?"
        )
