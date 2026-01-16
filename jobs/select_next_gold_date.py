import oci
import re
from datetime import datetime, timezone
from collections import defaultdict

# -----------------------------
# Config
# -----------------------------
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
RAW_BUCKET = "crypto-raw"
RAW_PREFIX = "binance/"

DATE_RE = re.compile(r"binance/([^/]+)/(\d{4}-\d{2}-\d{2})/.*\.parquet$")

# -----------------------------
# OCI client
# -----------------------------
config = oci.config.from_file()
client = oci.object_storage.ObjectStorageClient(config)
ns = client.get_namespace().data


# -----------------------------
# Helpers
# -----------------------------
def list_raw_objects():
    return client.list_objects(
        ns,
        RAW_BUCKET,
        prefix=RAW_PREFIX
    ).data.objects


def raw_dates_by_symbol():
    """
    Returns:
        dict[str, set[str]]
        {
            "2026-01-16": {"BTCUSDT", "ETHUSDT"},
            ...
        }
    """
    dates = defaultdict(set)

    for o in list_raw_objects():
        m = DATE_RE.match(o.name)
        if not m:
            continue

        symbol, date = m.groups()
        dates[date].add(symbol)

    return dates


# -----------------------------
# Selection logic
# -----------------------------
def select_next_date():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dates = raw_dates_by_symbol()

    # Dates that have data for all symbols
    valid_dates = sorted(
        d for d, syms in dates.items()
        if set(SYMBOLS).issubset(syms)
    )

    if not valid_dates:
        return None

    # Prefer today if it has data
    if today in valid_dates:
        return today

    # Otherwise return most recent past date
    return valid_dates[-1]


if __name__ == "__main__":
    date = select_next_date()
    if date:
        print(date)
