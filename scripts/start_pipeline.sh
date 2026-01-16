#!/bin/bash
set -e

echo "[INFO] Reloading systemd units"
sudo systemctl daemon-reload

echo "[INFO] Starting timers explicitly"

# Raw upload timer
sudo systemctl enable crypto-upload.timer
sudo systemctl start  crypto-upload.timer

sudo systemctl start crypto-upload.service

# Gold backfill timer
sudo systemctl enable gold-backfill.timer
sudo systemctl start  gold-backfill.timer

sudo systemctl start gold-backfill.service

echo "[INFO] Starting core long-running services"

# Real-time ingest (websocket)
sudo systemctl enable crypto-ingest.service
sudo systemctl start  crypto-ingest.service

echo "[INFO] Pipeline startup complete"

