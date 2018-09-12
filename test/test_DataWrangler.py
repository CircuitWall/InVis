from unittest import TestCase

from os.path import join, dirname

from pyspark.sql import dataframe

from iod.data import SparkDataWrangler


class TestSparkDataWrangler(TestCase):
    datawgl = SparkDataWrangler()

    def test_init(self):
        df: dataframe = self.datawgl.create_file_datasource(join(dirname(__file__), 'resource/diabetic_data.csv'))
        df.show(10)

    def test_filtering(self):
        df: dataframe = self.datawgl.create_file_datasource(join(dirname(__file__), 'resource/diabetic_data.csv'))
        df.show(10)
        df = self.datawgl.filter_datasource(data=df,
                                            include=['encounter_id', 'patient_nbr', 'race', 'gender', 'age', 'weight'],
                                            where="gender = 'Male'")
        df.show(10)

    def test_merging(self):
        diabetic: dataframe = self.datawgl.create_file_datasource(join(dirname(__file__), 'resource/diabetic_data.csv'))
        diabetic.show(10)
        diabetic: dataframe = self.datawgl.create_file_datasource(join(dirname(__file__), 'resource/insurance.csv'))
        diabetic.show(10)
