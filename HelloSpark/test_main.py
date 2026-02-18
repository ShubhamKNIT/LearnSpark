import pytest
from lib.utils import *
from pyspark.testing.utils import assertDataFrameEqual

@pytest.fixture(scope='session')
def spark():
	return get_spark_session("local")

@pytest.fixture(scope='session')
def survey_file():
	return "data/sample.csv"

def test_load_survey(spark, survey_file):
	expected_rows_count = load_survey_df(spark, survey_file).count()
	assert expected_rows_count == 9

def test_count_by_country(spark, survey_file):
	data_list = [("United States", 4), ("Canada", 2), ("United Kingdom", 1)]
	data_schema = "Country string, count long"
	expected_df = spark.createDataFrame(data_list, data_schema)

	raw_df = load_survey_df(spark, survey_file)
	actual_df = count_by_country(raw_df)

	assertDataFrameEqual(expected_df, actual_df)
