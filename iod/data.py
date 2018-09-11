from typing import List

from os.path import join, dirname, isfile

from os import listdir
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import dataframe


class DataWrangler:
    pass


class SparkDataWrangler(DataWrangler):
    def __init__(self, driver_dir='./driver'):
        self.driver_dir = join(dirname(__file__), driver_dir)
        self.sql_context = self.get_sql_context()

    def get_sql_context(self):
        drivers = [join(self.driver_dir, f) for f in listdir(self.driver_dir) if isfile(join(self.driver_dir, f))]
        conf = SparkConf().set("spark.jars", ','.join(drivers))
        sc = SparkContext.getOrCreate(conf=conf)
        return SQLContext(sc)

    def create_file_datasource(self, path: str, delimiter: str = ','):
        pass

    def merge_datasource(self, left, right, join_key_left: str, join_key_right: str = None,
                         join_mode: str = 'inner'):
        pass

    def filter_datasource(self, data, include: List[str] = list(), exclude: List[str] = list(),
                          where: str = None):
        pass


class PandasDataWrangler(DataWrangler):
    def create_file_datasource(self, path: str, delimiter: str = ','):
        pass

    def merge_datasource(self, left, right, join_key_left: str, join_key_right: str = None,
                         join_mode: str = 'inner'):
        pass

    def filter_datasource(self, data, include: List[str] = list(), exclude: List[str] = list(),
                          where: str = None):
        pass
