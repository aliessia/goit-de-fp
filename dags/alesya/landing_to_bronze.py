from pyspark.sql import SparkSession
import requests
import os


def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}.csv")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


def main():
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    for file_name in ["athlete_bio", "athlete_event_results"]:
        local_csv_path = f"data/landing/{file_name}"
        local_parquet_path = f"data/bronze/{file_name}"

        if not os.path.exists(local_csv_path + ".csv"):
            download_data(file_name)
            os.rename(file_name + ".csv", local_csv_path + ".csv")

        df = spark.read.option("header", True).csv(local_csv_path + ".csv")

        # Show DataFrame after reading CSV
        df.show()

        df.write.mode("overwrite").parquet(local_parquet_path)
        print(f"✅ Saved {file_name} to bronze layer")


if __name__ == "__main__":
    main()
