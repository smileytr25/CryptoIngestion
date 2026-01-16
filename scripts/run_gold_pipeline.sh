#!/bin/bash
set -euo pipefail
set -x

DATE="$1"

WORKDIR="/home/opc/CryptoIngestion"
cd "$WORKDIR"

if [[ -z "$DATE" ]]; then
  echo "[ERROR] DATE argument missing"
  exit 1
fi

SYMBOLS=("BTCUSDT" "ETHUSDT")

echo "[INFO] Gold pipeline starting for date: $DATE"
echo "[INFO] Symbols: ${SYMBOLS[*]}"

# -----------------------------
# Per-symbol processing
# -----------------------------
for SYMBOL in "${SYMBOLS[@]}"; do
  echo "[INFO] Processing symbol: $SYMBOL"

  python3 ./batch/temp_staging.py \
    --symbol "$SYMBOL" \
    --date "$DATE"

  python3 ./spark/gold/hourly_ohlc.py \
    --symbol "$SYMBOL" \
    --date "$DATE"

  python3 ./spark/gold/daily_ohlc.py \
    --symbol "$SYMBOL" \
    --date "$DATE"

  python3 ./spark/gold/monthly_ohlc.py \
    --symbol "$SYMBOL" \
    --date "$DATE"

  python3 ./spark/gold/feature_snapshot.py \
    --symbol "$SYMBOL" \
    --date "$DATE"
done

# -----------------------------
# Upload Gold outputs
# -----------------------------
if [[ ! -d "./spark/gold/tmp/gold" ]] || [[ -z "$(ls -A ./spark/gold/tmp/gold)" ]]; then
  echo "[ERROR] ./spark/gold/tmp/gold is missing or empty — aborting upload"
  exit 1
fi

echo "[INFO] Deleting existing gold data for date $DATE"

OCI=/usr/local/bin/oci   # adjust if needed

for SYMBOL IN "${SYMBOLS[@]}"; do
  $OCI os object bulk-delete \
    --bucket-name crypto-gold \
    --prefix daily_ohlc/binance/$SYMBOL/$DATE/ \
    --force || true

  $OCI os object bulk-delete \
    --bucket-name crypto-gold \
    --prefix hourly_ohlc/binance/$SYMBOL/$DATE/ \
    --force || true

  $OCI os object bulk-delete \
    --bucket-name crypto-gold \
    --prefix monthly_ohlc/binance/$SYMBOL/$DATE/ \
    --force || true

  $OCI os object bulk-delete \
    --bucket-name crypto-gold \
    --prefix features/binance/$SYMBOL/$DATE/ \
    --force || true
done

echo "[INFO] Uploading Gold data to object storage"

$OCI os object bulk-upload \
  --bucket-name crypto-gold \
  --src-dir ./spark/gold/tmp/gold \
  --overwrite \
  --exclude "*.crc" \
  --exclude "_SUCCESS" \
  --exclude "._SUCCESS"

# -----------------------------
# Cleanup local temp data
# -----------------------------
echo "[INFO] Cleaning up local temp directories"
rm -rf ./batch/tmp ./spark/gold/tmp

echo "[INFO] Gold pipeline completed successfully for date: $DATE"
