import websocket_client
from candle_buffer import CandleBuffer
from local_parquet_writer import LocalParquetWriter
import sys 
import signal 

candle_buffer = CandleBuffer()
parquet_writer = LocalParquetWriter(exchange_name="binance", base_path="./data")

def event_handler(event):
    candle_buffer.add(event)
    if candle_buffer.should_flush():
        flushed_data = candle_buffer.flush()
        parquet_writer.write(flushed_data)

def shutdown_handler(signal, frame):
    flushed_data = candle_buffer.flush()
    if flushed_data:
        parquet_writer.write(flushed_data)
    sys.exit(0)

websocket_client.EVENT_HANDLER = event_handler

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

websocket_client.run()