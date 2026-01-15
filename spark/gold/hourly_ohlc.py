import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, first, max, min, last, sum, avg

def main(symbol, date):
    spark = SparkSession.builder.appName('gold-hourly-ohlc').getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    staging = f"/tmp/staging/crypto-raw/binance/{symbol}/{date}/"
    out = f"/tmp/gold/hourly_ohlc/binance/{symbol}/{date}/"

    df = spark.read.parquet(staging)

    hourly = (
        df.withColumn("hour_ts", date_trunc("hour", col("close_time")))
          .groupBy("symbol", "hour_ts")
          .agg(
                first("open").alias("open"),
                avg("close").alias("avg_price"),
                max("high").alias("high"),
                min("low").alias("low"),
                last("close").alias("close"),
                sum("volume").alias("volume")
          )
    )

    hourly.write.mode("overwrite").partitionBy("hour_ts").parquet(out)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute hourly OHLC from raw minute data.")
    parser.add_argument("--symbol", type=str, required=True, help="Trading symbol (e.g., BTCUSDT)")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")

    args = parser.parse_args()
    main(args.symbol, args.date)