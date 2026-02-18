from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("K8sSparkDemo").getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

df_filtered = df.filter(col("age") > 30)

df_filtered.show()

spark.stop()
