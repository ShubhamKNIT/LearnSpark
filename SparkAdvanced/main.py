from pyspark.sql import SparkSession
# from pyspark import SparkConf
from lib.utils import get_spark_app_config, load_survey_df, count_by_country
from lib.logger import Log4j
import sys

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = (
        SparkSession.builder
            .config(conf=conf)
            .getOrCreate()
    )

    logger = Log4j(spark)
    logger.info("Spark Session created successfully.")

    # Processing logic goes here
    # conf_out = spark.sparkContext.getConf()
    # logger.debug(conf_out.toDebugString())

    survey_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    logger.info(count_df.collect())

    input("Press Enter")

    logger.info("Finishing the Spark application.")
    spark.stop()
