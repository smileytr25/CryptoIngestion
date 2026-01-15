import oci
import os
import argparse

def main(symbol: str, date: str):
    config = oci.config.from_file()
    client = oci.object_storage.ObjectStorageClient(config)
    namespace = client.get_namespace().data

    prefix = f"binance/{symbol}/{date}/"
    out = f"/tmp/staging/crypto-raw/binance/{symbol}/{date}/"

    os.makedirs(out, exist_ok=True)

    objs = client.list_objects(
        namespace,
        "crypto-raw",
        prefix=prefix
    ).data.objects

    if not objs:
        raise RuntimeError(f"No objects found for prefix: {prefix}")

    for o in objs:
        client.get_object(
            namespace,
            "crypto-raw",
            o.name,
            dest_file_path=os.path.join(out, os.path.basename(o.name))
        )

    print(f"Staged {len(objs)} files to {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage crypto raw data from OCI")
    parser.add_argument("--symbol", required=True, help="Symbol (e.g. BTCUSDT)")
    parser.add_argument("--date", required=True, help="Date (YYYY-MM-DD)")

    args = parser.parse_args()
    main(args.symbol, args.date)


