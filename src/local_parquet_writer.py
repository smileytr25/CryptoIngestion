import os
import uuid
import pandas as pd

class LocalParquetWriter:
    def __init__(self, base_path, exchange_name):
        self.base_path = base_path
        self.exchange_name = exchange_name
    def write(self, flushed_data):
        for symbol, events in flushed_data.items():
            if not events:
                continue

            df = pd.DataFrame(events)

            # Ensure datetime type (safe even if already correct)
            df["candle_start_time"] = pd.to_datetime(df["candle_start_time"], utc=True)

            # Group by candle date
            for date, date_df in df.groupby(df["candle_start_time"].dt.strftime("%Y-%m-%d")):
                symbol_path = os.path.join(
                    self.base_path,
                    self.exchange_name,
                    symbol,
                    date
                )

                os.makedirs(symbol_path, exist_ok=True)
                file_id = uuid.uuid4()
                temp_path = os.path.join(symbol_path, f"temp_{file_id}.parquet")
                final_path = os.path.join(symbol_path, f"{file_id}.parquet")

                try:
                    date_df.to_parquet(temp_path, index=False)
                    os.rename(temp_path, final_path)
                except Exception:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    raise