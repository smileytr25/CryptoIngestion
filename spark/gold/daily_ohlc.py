import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, first, max, min, last, sum, lag
from pyspark.sql.window import Window

def main(symbol, date):
    spark = SparkSession.builder.appName("gold-daily-ohlc").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    staging = f"batch/tmp/staging/crypto-raw/binance/{symbol}/{date}/"
    out = f"spark/gold/tmp/gold/daily_ohlc/binance/{symbol}/{date}/"

    df = spark.read.parquet(staging)
    df = df.withColumn("date", to_date(col("candle_close_time")))

    daily = (
        df.groupBy("symbol", "date")
          .agg(
            first("open").alias("open"),
            max("high").alias("high"),
            min("low").alias("low"),
            last("close").alias("close")
          )
    )

    w = Window.partitionBy("symbol").orderBy("date")

    daily = daily.withColumn(
        "returns",
        (col("close") - lag("close").over(w)) / lag("close").over(w)
    )

    spark.conf.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )

    daily.write.mode("overwrite").parquet(out)
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True, type=str, help="Trading symbol, e.g., BTCUSDT")
    p.add_argument("--date", required=True, type=str, help="Date in YYYY-MM-DD format")
    args = p.parse_args()
    main(args.symbol, args.date)