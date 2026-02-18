import configparser
from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for key, value in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, value)

    return spark_conf

def load_survey_df(spark, data_file):
    file_schema = """Timestamp TIMESTAMP,Age INT,Gender STRING,Country STRING,state STRING,self_employed STRING,
                family_history STRING,treatment STRING,work_interfere STRING,no_employees STRING,remote_work STRING,
                tech_company STRING,benefits STRING,care_options STRING,wellness_program STRING,seek_help STRING,anonymity STRING,
                leave STRING,mental_health_consequence STRING,phys_health_consequence STRING,coworkers STRING,
                supervisor STRING,mental_health_interview STRING,phys_health_interview STRING,mental_vs_physical STRING,
                obs_consequence STRING,comments STRING"""

    return (
        spark.read.format("csv")
            .option("header", "true")
            .schema(file_schema)
            .load(data_file)
    )

def count_by_country(survey_df):
    return (
        survey_df.where("Age < 40")
            .select("Timestamp","Age","Gender","Country","state")
            .groupBy("Country")
            .count()
    )
