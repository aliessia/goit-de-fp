from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    bio_df = spark.read.parquet("data/silver/athlete_bio")
    results_df = spark.read.parquet("data/silver/athlete_event_results")

    # Ensure numeric columns are numeric
    for col_name in ["height", "weight"]:
        bio_df = bio_df.withColumn(col_name, col(col_name).cast("double"))

    # Show bio_df after casting columns to double
    bio_df.show()

    # Join and aggregate
    joined_df = results_df.join(bio_df, on="athlete_id", how="inner")

    # Show joined dataframe
    joined_df.show()

    agg_df = joined_df.groupBy("athlete_id").agg(
        {"height": "avg", "weight": "avg"}
    ).withColumnRenamed("avg(height)", "avg_height")\
     .withColumnRenamed("avg(weight)", "avg_weight")

    # Show aggregated dataframe
    agg_df.show()

    # Save to the gold layer
    agg_df.write.mode("overwrite").parquet("data/gold/athlete_aggregates")
    print("✅ Saved final dataset to gold layer")

if __name__ == "__main__":
    main()
