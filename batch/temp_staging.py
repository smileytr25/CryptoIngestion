import oci
import os
import argparse
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from datetime import datetime, timedelta
from calendar import monthrange


# -------------------------------------------------
# Parquet rewrite for Spark compatibility
# -------------------------------------------------
def rewrite_parquet_for_spark(path: str):
    table = pq.read_table(path)
    df = table.to_pandas()

    # Convert datetime64[ns] → datetime64[us]
    for c in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[c] = df[c].astype("datetime64[us]")

    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        path,
        coerce_timestamps="us",
        allow_truncated_timestamps=True
    )


# -------------------------------------------------
# Download helper
# -------------------------------------------------
def download_prefix(client, namespace, bucket, prefix, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    objs = client.list_objects(
        namespace,
        bucket,
        prefix=prefix
    ).data.objects

    if not objs:
        return 0

    for o in objs:
        response = client.get_object(
            namespace_name=namespace,
            bucket_name=bucket,
            object_name=o.name
        )

        local_path = os.path.join(out_dir, os.path.basename(o.name))

        with open(local_path, "wb") as f:
            for chunk in response.data.raw.stream(1024 * 1024):
                f.write(chunk)

        rewrite_parquet_for_spark(local_path)

    return len(objs)


# -------------------------------------------------
# Main
# -------------------------------------------------
def main(symbol: str, date: str, month: str, lookback_days: int):

    config = oci.config.from_file()
    client = oci.object_storage.ObjectStorageClient(config)
    namespace = client.get_namespace().data
    bucket = "crypto-raw"

    base_out = f"batch/tmp/staging/crypto-raw/binance/{symbol}"

    total_files = 0

    # -------------------------------------------------
    # MODE 1: single day (hourly, daily)
    # -------------------------------------------------
    if date and not lookback_days:
        prefix = f"binance/{symbol}/{date}/"
        out = f"{base_out}/{date}/"

        total_files += download_prefix(
            client, namespace, bucket, prefix, out
        )

    # -------------------------------------------------
    # MODE 2: rolling lookback (features)
    # -------------------------------------------------
    elif date and lookback_days:
        end = datetime.strptime(date, "%Y-%m-%d")
        start = end - timedelta(days=lookback_days - 1)

        out = f"{base_out}/lookback_{lookback_days}/{date}/"
        os.makedirs(out, exist_ok=True)

        for i in range(lookback_days):
            d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            prefix = f"binance/{symbol}/{d}/"

            total_files += download_prefix(
                client, namespace, bucket, prefix, out
            )

    # -------------------------------------------------
    # MODE 3: full month (monthly OHLC)
    # -------------------------------------------------
    elif month:
        year, m = map(int, month.split("-"))
        _, last_day = monthrange(year, m)

        out = f"{base_out}/month/{month}/"
        os.makedirs(out, exist_ok=True)

        for day in range(1, last_day + 1):
            d = f"{year}-{m:02d}-{day:02d}"
            prefix = f"binance/{symbol}/{d}/"

            total_files += download_prefix(
                client, namespace, bucket, prefix, out
            )

    else:
        raise ValueError("Invalid staging arguments")

    if total_files == 0:
        raise RuntimeError("No objects staged — check inputs")

    print(f"[OK] Staged {total_files} files for {symbol}")


# -------------------------------------------------
# CLI
# -------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stage crypto raw data from OCI with variable lookback"
    )
    parser.add_argument("--symbol", required=True, help="Symbol (e.g. BTCUSDT)")
    parser.add_argument("--date", help="Date (YYYY-MM-DD)")
    parser.add_argument("--month", help="Month (YYYY-MM)")
    parser.add_argument(
        "--lookback-days",
        type=int,
        help="Rolling lookback window (e.g. 60)"
    )

    args = parser.parse_args()

    main(
        symbol=args.symbol,
        date=args.date,
        month=args.month,
        lookback_days=args.lookback_days
    )
