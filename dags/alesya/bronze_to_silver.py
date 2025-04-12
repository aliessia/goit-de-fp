from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    clean_text_udf = udf(clean_text, StringType())

    for file_name in ["athlete_bio", "athlete_event_results"]:
        input_path = f"data/bronze/{file_name}"
        output_path = f"data/silver/{file_name}"

        df = spark.read.parquet(input_path)

        # Clean all string columns
        for column in df.columns:
            if dict(df.dtypes)[column] == "string":
                df = df.withColumn(column, clean_text_udf(col(column)))

        # Show DataFrame after cleaning text
        df.show()

        # Drop duplicates
        df = df.dropDuplicates()

        df.write.mode("overwrite").parquet(output_path)
        print(f"✅ Saved {file_name} to silver layer")

if __name__ == "__main__":
    main()
