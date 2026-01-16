#!/bin/bash
set -e

echo "[INFO] Reloading systemd units"
sudo systemctl daemon-reload

echo "[INFO] Starting timers explicitly"

# Raw upload timer
sudo systemctl disable crypto-upload.timer
sudo systemctl stop  crypto-upload.timer

# Gold backfill timer
sudo systemctl disable gold-backfill.timer
sudo systemctl stop  gold-backfill.timer

echo "[INFO] Starting core long-running services"

# Real-time ingest (websocket)
sudo systemctl disable crypto-ingest.service
sudo systemctl stop crypto-ingest.service

echo "[INFO] Pipeline stopped"