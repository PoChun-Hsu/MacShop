from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("HelloSpark") \
        .getOrCreate()

    data = [("MacShop", 1), ("PTT", 2), ("Airflow", 3)]
    df = spark.createDataFrame(data, ["source", "count"])

    df.show()

    spark.stop()
