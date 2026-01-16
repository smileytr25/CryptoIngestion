import oci
import os
import argparse
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

def rewrite_parquet_for_spark(path):
    table = pq.read_table(path)
    df = table.to_pandas()

    # Convert all datetime64[ns] → datetime64[us]
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].astype("datetime64[us]")

    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        path,
        coerce_timestamps="us",
        allow_truncated_timestamps=True
    )

def main(symbol: str, date: str):
    config = oci.config.from_file()
    client = oci.object_storage.ObjectStorageClient(config)
    namespace = client.get_namespace().data

    prefix = f"binance/{symbol}/{date}/"
    out = f"batch/tmp/staging/crypto-raw/binance/{symbol}/{date}/"

    os.makedirs(out, exist_ok=True)

    objs = client.list_objects(
        namespace,
        "crypto-raw",
        prefix=prefix
    ).data.objects

    if not objs:
        raise RuntimeError(f"No objects found for prefix: {prefix}")

    for o in objs:
        response = client.get_object(
                namespace_name=namespace,
                bucket_name="crypto-raw",
                object_name=o.name
            )
        
        local_path = os.path.join(out, os.path.basename(o.name))

        with open(local_path, "wb") as f:
            for chunk in response.data.raw.stream(1024 * 1024):
                f.write(chunk)

        rewrite_parquet_for_spark(local_path)
        
    print(f"Staged {len(objs)} files to {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage crypto raw data from OCI")
    parser.add_argument("--symbol", required=True, help="Symbol (e.g. BTCUSDT)")
    parser.add_argument("--date", required=True, help="Date (YYYY-MM-DD)")

    args = parser.parse_args()
    main(args.symbol, args.date)


