import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, lag, log, hour,
    dayofweek, when, greatest, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DoubleType


# ---------------------------
# Pandas EMA function
# ---------------------------
def compute_ema(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("candle_close_time")

    pdf["ema_5"]  = pdf["close"].ewm(span=5, adjust=False).mean()
    pdf["ema_15"] = pdf["close"].ewm(span=15, adjust=False).mean()
    pdf["ema_30"] = pdf["close"].ewm(span=30, adjust=False).mean()
    pdf["ema_60"] = pdf["close"].ewm(span=60, adjust=False).mean()

    return pdf


# ---------------------------
# Main
# ---------------------------
def main(symbol: str, date: str):

    spark = (
        SparkSession.builder
        .appName("gold-feature-snapshot")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    staging = f"batch/tmp/staging/crypto-raw/binance/{symbol}/{date}/"
    out = f"spark/gold/tmp/gold/features/binance/{symbol}/{date}/"

    df = spark.read.parquet(staging)

    # ---------------------------
    # Windows (NO leakage)
    # ---------------------------
    w1  = Window.partitionBy("symbol").orderBy("candle_close_time")
    w5  = w1.rowsBetween(-5, -1)
    w15 = w1.rowsBetween(-15, -1)
    w30 = w1.rowsBetween(-30, -1)
    w60 = w1.rowsBetween(-60, -1)
    w14 = w1.rowsBetween(-14, -1)

    # ---------------------------
    # Feature engineering (Spark-native)
    # ---------------------------
    features = (
        df
        # returns
        .withColumn("log_return_1", log(col("close") / lag("close").over(w1)))
        .withColumn(
            "return_5",
            (col("close") - lag("close", 5).over(w1)) /
            lag("close", 5).over(w1)
        )

        # moving averages
        .withColumn("ma_5",  avg("close").over(w5))
        .withColumn("ma_15", avg("close").over(w15))
        .withColumn("ma_30", avg("close").over(w30))
        .withColumn("ma_60", avg("close").over(w60))

        # volatility
        .withColumn("vol_5",  stddev("log_return_1").over(w5))
        .withColumn("vol_15", stddev("log_return_1").over(w15))
        .withColumn("vol_30", stddev("log_return_1").over(w30))
        .withColumn("vol_60", stddev("log_return_1").over(w60))

        # RSI
        .withColumn("delta", col("close") - lag("close").over(w1))
        .withColumn("gain", greatest(col("delta"), lit(0)))
        .withColumn("loss", greatest(-col("delta"), lit(0)))
        .withColumn("avg_gain_14", avg("gain").over(w14))
        .withColumn("avg_loss_14", avg("loss").over(w14))
        .withColumn(
            "rsi_14",
            when(col("avg_loss_14") == 0, lit(100.0))
            .otherwise(100 - (100 / (1 + col("avg_gain_14") / col("avg_loss_14"))))
        )

        # time features
        .withColumn("hour_of_day", hour("candle_close_time"))
        .withColumn("day_of_week", dayofweek("candle_close_time"))
    )

    # ---------------------------
    # EMA (stateful → Pandas)
    # ---------------------------
    ema_schema = StructType(features.schema.fields + [
        StructField("ema_5",  DoubleType()),
        StructField("ema_15", DoubleType()),
        StructField("ema_30", DoubleType()),
        StructField("ema_60", DoubleType()),
    ])

    features = (
        features
        .groupBy("symbol")
        .applyInPandas(compute_ema, schema=ema_schema)
    )

    # ---------------------------
    # Final select
    # ---------------------------
    final = features.select(
        "symbol",
        col("candle_close_time").alias("ts"),
        "close",
        "log_return_1",
        "return_5",
        "ma_5", "ma_15", "ma_30", "ma_60",
        "ema_5", "ema_15", "ema_30", "ema_60",
        "vol_5", "vol_15", "vol_30", "vol_60",
        "rsi_14",
        "hour_of_day",
        "day_of_week"
    )

    spark.conf.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )

    final.write.mode("overwrite").parquet(out)
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--date", required=True)
    args = p.parse_args()
    main(args.symbol, args.date)
