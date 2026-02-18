from databricks.connect import DatabricksSession
import os

host = os.environ["DATABRICKS_HOST"]
token = os.environ["DATABRICKS_TOKEN"]

spark = (
	DatabricksSession.builder
		.host(host)
		.token(token)
		.serverless()
		.getOrCreate()
)

df = spark.read.table("dev.spark_db.diamonds")
df.show(3)
