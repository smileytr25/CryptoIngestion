import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trunc,
    avg,
    max,
    min,
    first,
    last
)

def main(symbol, date):
    spark = (
        SparkSession
        .builder
        .appName("gold-monthly-ohlc")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    # -----------------------------
    # Date parsing
    # -----------------------------
    year = date[:4]
    month = date[5:7]
    year_month = f"{year}-{month}"

    # -----------------------------
    # Paths
    # -----------------------------
    staging = f"batch/tmp/staging/crypto-raw/binance/{symbol}/month/{year_month}/"

    # CRITICAL FIX: month-level output path
    out = f"spark/gold/tmp/gold/monthly_ohlc/binance/{symbol}/{year_month}/"

    # -----------------------------
    # Read staging
    # -----------------------------
    df = spark.read.parquet(staging)

    # Normalize to month boundary
    df = df.withColumn(
        "month",
        trunc(col("candle_close_time"), "month")
    )

    # -----------------------------
    # Monthly aggregation
    # -----------------------------
    monthly = (
        df
        .orderBy("candle_close_time")  # REQUIRED for correct OHLC
        .groupBy("symbol", "month")
        .agg(
            first("open").alias("open"),
            max("high").alias("high"),
            min("low").alias("low"),
            avg("close").alias("avg_price"),
            last("close").alias("close")
        )
    )

    # -----------------------------
    # Write (ONE FILE, OVERWRITE)
    # -----------------------------
    (
        monthly
        .coalesce(1)              # CRITICAL: single parquet file
        .write
        .mode("overwrite")
        .parquet(out)
    )

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Trading symbol (e.g., BTCUSDT)"
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Date in YYYY-MM-DD format"
    )
    args = parser.parse_args()

    main(args.symbol, args.date)
