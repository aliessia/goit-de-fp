from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_event_results"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

spark = (SparkSession.builder
    .appName("MySQLSparkToKafka")
    .config("spark.jars", ",".join([
        "mysql-connector-j-8.0.32.jar",
        "spark-sql-kafka-0-10_2.12-3.5.0.jar",
        "kafka-clients-3.5.0.jar",
        "spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
        "commons-pool2-2.11.1.jar"
    ]))
    .getOrCreate())

df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password
).load()

df.show(10)

df_with_json = df.withColumn("value", to_json(struct(*df.columns)))

df_to_kafka = df_with_json.withColumn("key", col("athlete_id").cast("string")) \
                          .select("key", "value")

df_to_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "athlete_event_results") \
    .save()
