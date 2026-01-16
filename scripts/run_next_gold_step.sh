#!/bin/bash
set -euo pipefail

WORKDIR="/home/opc/CryptoIngestion"
cd "$WORKDIR"

# -----------------------------
# Select date to process
# -----------------------------
DATE=$(python3 ./jobs/select_next_gold_date.py)

if [[ -z "$DATE" ]]; then
  echo "[INFO] No eligible Gold date to process"
  exit 0
fi

# Validate date format (YYYY-MM-DD)
if [[ ! "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "[ERROR] Invalid date returned: $DATE"
  exit 1
fi

echo "[INFO] Starting Gold pipeline for date: $DATE"

# -----------------------------
# Run Gold pipeline
# -----------------------------
./scripts/run_gold_pipeline.sh "$DATE"

echo "[INFO] Finished Gold pipeline for date: $DATE"
