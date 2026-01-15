#!/bin/bash
set -e

DATE="$1"
SYMBOLS=("BTCUSDT" "ETHUSDT")

for SYMBOL in "${SYMBOLS[@]}"; do
    python3 temp_staging.py --symbol "$SYMBOL" --date "$DATE"

    python3 hourly_ohlc.py      --symbol "$SYMBOL" --date "$DATE"
    python3 daily_ohlc.py       --symbol "$SYMBOL" --date "$DATE"
    python3 monthly_ohlc.py     --symbol "$SYMBOL" --date "$DATE"
    python3 feature_snapshot.py --symbol "$SYMBOL" --date "$DATE"
done

# upload gold
oci os object bulk-upload \
    --bucket-name crypto-gold \
    --src-dir /tmp/gold \
    --overwrite

# mark date processed (CRITICAL)
echo "ok" > /tmp/processed.txt
oci os object put \
    --bucket-name crypto-gold \
    --name "_processed_dates/${DATE}.done" \
    --file /tmp/processed.txt

# cleanup local temp
rm -rf /tmp/staging /tmp/gold
