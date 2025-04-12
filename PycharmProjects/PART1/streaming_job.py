from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, avg, to_json, struct
from pyspark.sql.types import StructType, StringType, IntegerType


jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"


spark = (SparkSession.builder
    .appName("KafkaStreamProcessor")
    .config("spark.jars", ",".join([
        "mysql-connector-j-8.0.32.jar",
        "spark-sql-kafka-0-10_2.12-3.5.0.jar",
        "kafka-clients-3.5.0.jar",
        "spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
        "commons-pool2-2.11.1.jar"
    ]))
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")


schema = StructType() \
    .add("edition", StringType()) \
    .add("edition_id", IntegerType()) \
    .add("country_noc", StringType()) \
    .add("sport", StringType()) \
    .add("event", StringType()) \
    .add("result_id", IntegerType()) \
    .add("athlete", StringType()) \
    .add("athlete_id", IntegerType()) \
    .add("pos", StringType()) \
    .add("medal", StringType()) \
    .add("isTeamSport", StringType())


kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "athlete_event_results")
    .load())


parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

bio_df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password
).load()


bio_clean = bio_df \
    .filter(col("height").isNotNull() & col("weight").isNotNull()) \
    .filter(col("height").cast("double").isNotNull()) \
    .filter(col("weight").cast("double").isNotNull())


joined_df = parsed_df.join(bio_clean, on="athlete_id", how="inner") \
    .select(
        "athlete_id", "sport", "medal",
        bio_clean["sex"].alias("gender"),
        bio_clean["country_noc"],
        bio_clean["height"],
        bio_clean["weight"]
    )


aggregated = joined_df.groupBy("sport", "medal", "gender", "country_noc") \
    .agg(
        avg(col("height")).alias("avg_height"),
        avg(col("weight")).alias("avg_weight")
    ).withColumn("timestamp", current_timestamp())


def write_to_sinks(batch_df, epoch_id):
    print(f"🌀 Epoch {epoch_id} with {batch_df.count()} rows")


    kafka_ready = batch_df.withColumn("value", to_json(struct(
        "sport", "medal", "gender", "country_noc", "avg_height", "avg_weight", "timestamp"
    ))).withColumn("key", col("sport").cast("string")) \
     .selectExpr("key", "value")

    kafka_ready.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_sport_avg") \
        .save()


    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "athlete_aggregates") \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()


query = (aggregated.writeStream
    .outputMode("complete")
    .foreachBatch(write_to_sinks)
    .start())

query.awaitTermination()
