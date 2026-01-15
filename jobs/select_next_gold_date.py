import oci
import re

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

RAW_BUCKET = "crypto-raw"
GOLD_BUCKET = "crypto-gold"

RAW_PREFIX = "binance/"
SUCCESS_FILE = "_SUCCESS"
GOLD_DONE_PREFIX = "_processed_dates/"

config = oci.config.from_file()
client = oci.object_storage.ObjectStorageClient(config)
ns = client.get_namespace().data

def list_raw_dates():
    objs = client.list_objects(
        ns,
        RAW_BUCKET,
        prefix=RAW_PREFIX
    ).data.objects

    dates = set()
    for o in objs:
        m = re.search(r"/(\d{4}-\d{2}-\d{2})/", o.name)
        if m:
            dates.add(m.group(1))

    return sorted(dates)


def has_success_for_all_symbols(date):
    for symbol in SYMBOLS:
        prefix = f"binance/{symbol}/{date}/{SUCCESS_FILE}"
        objs = client.list_objects(
            ns,
            RAW_BUCKET,
            prefix=prefix
        ).data.objects
        if not objs:
            return False
    return True


def already_processed(date):
    objs = client.list_objects(
        ns,
        GOLD_BUCKET,
        prefix=f"{GOLD_DONE_PREFIX}{date}.done"
    ).data.objects
    return len(objs) > 0


def select_next_date():
    for date in list_raw_dates():
        if not has_success_for_all_symbols(date):
            continue
        if already_processed(date):
            continue
        return date
    return None


if __name__ == "__main__":
    date = select_next_date()
    if date:
        print(date)
