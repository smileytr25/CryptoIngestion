import json 
import websocket
import time
from datetime import datetime, timezone

BINANCE_WS_URL = "wss://stream.binance.us:9443/stream"

SYMBOLS = ["btcusdt", "ethusdt"]
INTERVAL = "1m"

EVENT_HANDLER = None

def build_streams(symbols, interval):
    return [f"{s}@kline_{interval}" for s in symbols]

def on_message(ws, message):
    data = json.loads(message)

    kline = data.get("data", {}).get("k", {})
    if not kline:
        return 
    
    if not kline["x"]:
        return 
    
    event = {
        "exchange": "binance",
        "symbol": kline["s"],
        "interval": kline["i"],
        "event_time": datetime.fromtimestamp(data["data"]["E"] / 1000, tz=timezone.utc),
        "candle_start_time": datetime.fromtimestamp(kline["t"] / 1000, tz=timezone.utc),
        "candle_close_time": datetime.fromtimestamp(kline["T"] / 1000, tz=timezone.utc),
        "open": float(kline["o"]),
        "high": float(kline["h"]),
        "low": float(kline["l"]),
        "close": float(kline["c"]),
        "is_closed": True,
        "ingestion_timestamp": datetime.now(tz=timezone.utc)
    }

    if EVENT_HANDLER:
        EVENT_HANDLER(event)

def on_error(ws, error):
    print(f"WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed", close_status_code, close_msg)

def on_open(ws):
    streams = build_streams(SYMBOLS, INTERVAL)
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))
    print("Subscribed to streams:", streams)

def run():
    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("Reconnecting after error:", e)
            time.sleep(5)