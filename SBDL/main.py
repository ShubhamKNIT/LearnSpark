import sys
import uuid

from pyspark.sql.functions import struct, col, to_json

from lib import ConfigLoader, Utils, DataLoader, Transformations
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = "SBDL-" + str(uuid.uuid4())

    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id)
    conf = ConfigLoader.get_config(job_run_env)
    enable_hive = True if conf["enable.hive"] == "true" else False
    hive_db = conf["hive.database"]

    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)

    logger.info("Reading SBDL Account DF")
    accounts_df = DataLoader.read_accounts(spark, job_run_env, enable_hive, hive_db)
    contract_df = Transformations.get_contract(accounts_df)

    logger.info("Reading SBDL Party DF")
    parties_df = DataLoader.read_parties(spark, job_run_env, enable_hive, hive_db)
    relations_df = Transformations.get_relations(parties_df)

    logger.info("Reading SBDL Address DF")
    address_df = DataLoader.read_address(spark, job_run_env, enable_hive, hive_db)
    relation_address_df = Transformations.get_address(address_df)

    logger.info("Join Party Relations and Address")
    party_address_df = Transformations.join_party_address(relations_df, relation_address_df)

    logger.info("Join Account and Parties")
    data_df = Transformations.join_contract_party(contract_df, party_address_df)

    logger.info("Apply Header and create Event")
    final_df = Transformations.apply_header(spark, data_df)
    logger.info("Preparing to send data to Kafka")
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"),
                                  to_json(struct("*")).alias("value"))
    # input("Press Any Key")
    # kafka_kv_df.write.format("noop").mode("overwrite").save("test_data\noop")

    # Keep it in vault or other secure place, authorize application to extract it from there
    # api_key = conf["kafka.api_key"]
    # api_secret = conf["kafka.api_secret"]

    # kafka_username = conf["kafka.username"]
    # kafka_password = conf["kafka.password"]

    # Kafka SASL_SSL Configuration
    kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        .option("topic", conf["kafka.topic"]) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.scram.ScramLoginModule required username='{conf['kafka.username']}' password='{conf['kafka.password']}';") \
        .option("kafka.ssl.truststore.location", conf["kafka.ssl.truststore.location"]) \
        .option("kafka.ssl.truststore.password", conf["kafka.ssl.truststore.password"]) \
        .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
        .option("kafka.receive.buffer.bytes", 1048576) \
        .option("kafka.send.buffer.bytes", 1048576) \
        .save()

    logger.info("Finished sending data to Kafka")

    spark.stop()
    logger.info("Stopping the spark session, Job Finished")
