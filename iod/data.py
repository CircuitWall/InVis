from typing import List

from os.path import join, dirname, isfile

from os import listdir

from pandas import DataFrame
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import dataframe

driver = {
    'jdbc:mysql': 'com.mysql.cj.jdbc.Driver',
    'jdbc:teradata': 'com.teradata.jdbc.TeraDriver'
}


class DataWrangler:
    pass


class SparkDataWrangler(DataWrangler):
    def __init__(self, driver_dir='./driver'):
        self.driver_dir = join(dirname(__file__), driver_dir)
        self.sql_context = self.get_sql_context()

    def get_sql_context(self):
        drivers = [join(self.driver_dir, f) for f in listdir(self.driver_dir) if isfile(join(self.driver_dir, f))]
        conf = SparkConf().set("spark.jars", ','.join(drivers)).setAppName('DataWrangler')
        sc = SparkContext.getOrCreate(conf=conf)
        return SQLContext(sc)

    def create_file_datasource(self, path: str, delimiter: str = ','):
        return self.sql_context.read.load(path,
                                          format='com.databricks.spark.csv',
                                          header='true',
                                          inferSchema='true')

    def merge_datasource(self, left, right, on: str,
                         join_mode: str = 'inner'):
        left = self._convert_dataframe(left)
        right = self._convert_dataframe(right)
        left.join(right, on=on, how=join_mode)
        pass

    def lower_columns(self, data, *args):
        data = self._convert_dataframe(data)
        for value in args:
            data = data.withColumn(value, lower(col(value)))
        return data

    def upper_columns(self, data, **kwargs):
        data = self._convert_dataframe(data)
        for key, value in kwargs.items():
            data = data.withColumnRenamed(key, value)
        return data

    def rename_columns(self, data, **kwargs):
        data = self._convert_dataframe(data)
        for key, value in kwargs.items():
            data = data.withColumnRenamed(key, value)
        return data

    def filter_datasource(self, data, include: List[str] = None, exclude: List[str] = None,
                          where: str = None):
        data = self._convert_dataframe(data)
        if include is not None:
            # Opt in columns
            data = data.select(include)
        elif exclude is not None:
            # Opt out coulumns
            data = data.drop(exclude)
        else:
            print("No change.")
        return data.filter(where)

    def _convert_dataframe(self, data) -> dataframe:
        if isinstance(data, DataFrame):
            data = self.sql_context.createDataFrame(data)
        return data


class PandasDataWrangler(DataWrangler):
    def create_file_datasource(self, path: str, delimiter: str = ','):
        pass

    def merge_datasource(self, left, right, join_key_left: str, join_key_right: str = None,
                         join_mode: str = 'inner'):
        pass

    def filter_datasource(self, data, include: List[str] = list(), exclude: List[str] = list(),
                          where: str = None):
        pass
