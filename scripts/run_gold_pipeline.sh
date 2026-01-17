#!/bin/bash
set -euo pipefail
set -x

DATE="$1"

WORKDIR="/home/opc/CryptoIngestion"
cd "$WORKDIR"

if [[ -z "$DATE" ]]; then
  echo "[ERROR] DATE argument missing (YYYY-MM-DD)"
  exit 1
fi

# -----------------------------
# Date parsing
# -----------------------------
YEAR="${DATE:0:4}"
MONTH="${DATE:5:2}"
YEAR_MONTH="${YEAR}-${MONTH}"

SYMBOLS=("BTCUSDT" "ETHUSDT")

OCI="$HOME/.local/bin/oci"
BUCKET="crypto-gold"

echo "[INFO] Gold pipeline starting for date: $DATE"
echo "[INFO] Year=$YEAR Month=$MONTH"
echo "[INFO] Symbols: ${SYMBOLS[*]}"

# -----------------------------
# Per-symbol processing
# -----------------------------
for SYMBOL in "${SYMBOLS[@]}"; do
  echo "[INFO] Processing symbol: $SYMBOL"

  # -------------------------------------------------
  # 1. Stage ONE DAY (hourly + daily)
  # -------------------------------------------------
  python3 ./batch/temp_staging.py \
    --symbol "$SYMBOL" \
    --date "$DATE"

  python3 ./spark/gold/hourly_ohlc.py \
    --symbol "$SYMBOL" \
    --date "$DATE"

  python3 ./spark/gold/daily_ohlc.py \
    --symbol "$SYMBOL" \
    --date "$DATE"

  # -------------------------------------------------
  # 2. Stage FULL MONTH (monthly OHLC)
  # -------------------------------------------------
  python3 ./batch/temp_staging.py \
    --symbol "$SYMBOL" \
    --month "$YEAR_MONTH"

  python3 ./spark/gold/monthly_ohlc.py \
    --symbol "$SYMBOL" \
    --date "$DATE"

  # -------------------------------------------------
  # 3. Stage 60-DAY LOOKBACK (features)
  # -------------------------------------------------
  python3 ./batch/temp_staging.py \
    --symbol "$SYMBOL" \
    --date "$DATE" \
    --lookback-days 60

  python3 ./spark/gold/feature_snapshot.py \
    --symbol "$SYMBOL" \
    --date "$DATE"

done

# -----------------------------
# Validate local gold output
# -----------------------------
GOLD_TMP="./spark/gold/tmp/gold"

if [[ ! -d "$GOLD_TMP" ]] || [[ -z "$(ls -A "$GOLD_TMP")" ]]; then
  echo "[ERROR] Gold output directory missing or empty — aborting upload"
  exit 1
fi

# -----------------------------
# Delete remote targets (scoped)
# -----------------------------
echo "[INFO] Deleting existing gold data (scoped)"

for SYMBOL in "${SYMBOLS[@]}"; do

  # Hourly + Daily are DATE scoped
  $OCI os object bulk-delete \
    --bucket-name "$BUCKET" \
    --prefix "hourly_ohlc/binance/${SYMBOL}/${DATE}/" \
    --force || true

  $OCI os object bulk-delete \
    --bucket-name "$BUCKET" \
    --prefix "daily_ohlc/binance/${SYMBOL}/${DATE}/" \
    --force || true

  # Monthly is MONTH scoped
  $OCI os object bulk-delete \
    --bucket-name "$BUCKET" \
    --prefix "monthly_ohlc/binance/${SYMBOL}/${YEAR_MONTH}/" \
    --force || true

  # Feature snapshot is DATE scoped
  $OCI os object bulk-delete \
    --bucket-name "$BUCKET" \
    --prefix "features/binance/${SYMBOL}/${DATE}/" \
    --force || true

done

# -----------------------------
# Upload gold data
# -----------------------------
echo "[INFO] Uploading Gold data to object storage"

$OCI os object bulk-upload \
  --bucket-name "$BUCKET" \
  --src-dir "$GOLD_TMP" \
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
