import argparse 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trunc, avg, max, min, sum, first, last

def main(symbol, date):
    spark = SparkSession.builder.appName("gold-monthly-ohlc").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    staging = f"batch/tmp/staging/crypto-raw/binance/{symbol}/{date}/"
    out = f"spark/gold/tmp/gold/monthly_ohlc/binance/{symbol}/{date}/"

    df = spark.read.parquet(staging)
    df = df.withColumn("month", trunc(col("candle_close_time"), "month"))

    monthly = (
        df.groupBy("symbol", "month")
        .agg(
            first("open").alias("open"),
            avg("close").alias("avg_price"),
            max("high").alias("high"),
            min("low").alias("low"),
            last("close").alias("close")
        )
    )

    spark.conf.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
    
    monthly.write.mode("overwrite").parquet(out)
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", type=str, required=True, help="Trading symbol (e.g., BTCUSDT)")
    p.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = p.parse_args()
    main(args.symbol, args.date)