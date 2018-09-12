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
        df = self.datawgl.transform_datasource(data=df,
                                               include=['encounter_id', 'patient_nbr', 'race', 'upper(gender)', 'age',
                                                        'weight'],
                                               where="gender = 'Male'")
        df.show(10)

    def test_merging(self):
        diabetic: dataframe = self.datawgl.create_file_datasource(join(dirname(__file__), 'resource/diabetic_data.csv'))

        diabetic = self.datawgl.transform_datasource(data=diabetic,
                                                     include=['encounter_id', 'patient_nbr', 'race',
                                                              'upper(gender) as gender',
                                                              'age', 'weight'])
        diabetic.show(10)
        insurance: dataframe = self.datawgl.create_file_datasource(join(dirname(__file__), 'resource/insurance.csv'))

        insurance = self.datawgl.transform_datasource(data=insurance,
                                                      include=['age', 'upper(sex) as gender', 'children', 'region',
                                                               'charges', 'smoker'])
        insurance.show(10)
        merged = self.datawgl.merge_datasource(left=diabetic, right=insurance, on='gender')
        merged.show(10)
