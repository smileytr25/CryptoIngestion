# CryptoIngestion

A real-time cryptocurrency market data ingestion system that streams kline (candlestick) data from Binance US WebSocket API and stores it efficiently in Parquet format.

## Overview

This project connects to Binance US WebSocket streams to receive real-time 1-minute candlestick data for BTC/USDT and ETH/USDT trading pairs. The data is buffered in memory and periodically flushed to local Parquet files, organized by exchange, symbol, and date.

## Features

- **Real-time WebSocket Streaming**: Connects to Binance US WebSocket API with automatic reconnection
- **Buffered Ingestion**: In-memory buffer with configurable flush intervals (time-based or size-based)
- **Parquet Storage**: Efficient columnar storage format optimized for analytics
- **Date Partitioning**: Data organized by exchange/symbol/date for easy querying
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals to flush remaining data before exit
- **Systemd Integration**: Production-ready systemd service configuration included

## Architecture

```
WebSocket Stream → CandleBuffer → LocalParquetWriter → Parquet Files
     (Binance)      (In-memory)      (Batch writer)      (./data/...)
```

### Components

- **`websocket_client.py`**: Manages WebSocket connection to Binance, handles reconnection, and parses kline events
- **`candle_buffer.py`**: In-memory buffer that accumulates events and triggers flushes based on time (60s) or size (100 events)
- **`local_parquet_writer.py`**: Writes buffered data to Parquet files with date-based partitioning
- **`main.py`**: Entry point that wires components together and handles shutdown signals

## Installation

### Prerequisites

- Python 3.7+
- pip

### Setup

1. Clone the repository:
```bash
git clone https://github.com/smileytr25/CryptoIngestion.git
cd CryptoIngestion
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Local Execution

Run the ingestion service locally:

```bash
./scripts/run_local.sh
```

Or directly:

```bash
python src/main.py
```

### Production Deployment (systemd)

1. Copy the systemd service file:
```bash
sudo cp infra/systemd/crypto-ingest.service /etc/systemd/system/
```

2. Update the service file paths to match your installation directory

3. Enable and start the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable crypto-ingest.service
sudo systemctl start crypto-ingest.service
```

4. Check status:
```bash
sudo systemctl status crypto-ingest.service
```

## Configuration

### Symbols and Intervals

Edit `src/websocket_client.py` to configure trading pairs and intervals:

```python
SYMBOLS = ["btcusdt", "ethusdt"]  # Add more symbols here
INTERVAL = "1m"                    # 1m, 5m, 15m, 1h, etc.
```

### Buffer Settings

Adjust buffer behavior in `src/main.py`:

```python
candle_buffer = CandleBuffer(
    flush_interval=60,  # Seconds between flushes
    max_size=100        # Max events per symbol before flush
)
```

### Storage Path

Change the output directory in `src/main.py`:

```python
parquet_writer = LocalParquetWriter(
    exchange_name="binance",
    base_path="./data"  # Change storage location here
)
```

## Data Format

### Output Structure

```
./data/
└── binance/
    ├── BTCUSDT/
    │   ├── 2026-01-13/
    │   │   ├── <uuid1>.parquet
    │   │   └── <uuid2>.parquet
    │   └── 2026-01-14/
    │       └── <uuid3>.parquet
    └── ETHUSDT/
        └── 2026-01-13/
            └── <uuid4>.parquet
```

### Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| exchange | string | Exchange name (e.g., "binance") |
| symbol | string | Trading pair (e.g., "BTCUSDT") |
| interval | string | Candlestick interval (e.g., "1m") |
| event_time | timestamp | Event timestamp from exchange |
| candle_start_time | timestamp | Candle open time |
| candle_close_time | timestamp | Candle close time |
| open | float | Opening price |
| high | float | Highest price |
| low | float | Lowest price |
| close | float | Closing price |
| is_closed | boolean | Whether candle is finalized |
| ingestion_timestamp | timestamp | Time data was ingested |

## Querying Data

Use DuckDB (included in requirements) to query the Parquet files:

```python
import duckdb

# Query all BTC data for a specific date
result = duckdb.query("""
    SELECT * FROM 'data/binance/BTCUSDT/2026-01-13/*.parquet'
    ORDER BY candle_start_time
""").df()

# Aggregate across multiple days
result = duckdb.query("""
    SELECT 
        DATE(candle_start_time) as date,
        COUNT(*) as candle_count,
        MIN(low) as daily_low,
        MAX(high) as daily_high
    FROM 'data/binance/BTCUSDT/*/*.parquet'
    GROUP BY date
""").df()
```

## Development

### Project Structure

```
CryptoIngestion/
├── src/
│   ├── __init__.py
│   ├── main.py                    # Entry point
│   ├── websocket_client.py        # WebSocket client
│   ├── candle_buffer.py           # In-memory buffer
│   └── local_parquet_writer.py    # Parquet writer
├── scripts/
│   └── run_local.sh               # Local execution script
├── infra/
│   └── systemd/
│       └── crypto-ingest.service  # systemd service config
├── requirements.txt               # Python dependencies
├── .gitignore
└── README.md
```

## Dependencies

- **websocket-client**: WebSocket client library
- **pandas**: Data manipulation and DataFrame support
- **pyarrow**: Parquet file format support
- **duckdb**: SQL analytics engine for querying Parquet files

## License

This project is open source and available under the MIT License.

## Future Enhancements

- [ ] Support for multiple exchanges (Coinbase, Kraken, etc.)
- [ ] Cloud storage integration (S3, GCS)
- [ ] Data validation and quality checks
- [ ] Metrics and monitoring (Prometheus/Grafana)
- [ ] Historical data backfill capability
- [ ] Docker containerization
- [ ] Unit and integration tests