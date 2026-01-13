from collections import defaultdict
from datetime import datetime, timezone

class CandleBuffer:
    def __init__(self, flush_interval=60, max_size=100):
        self.buffer = defaultdict(list)
        self.flush_interval = flush_interval
        self.max_size = max_size
        self.last_flush_time = datetime.now(tz=timezone.utc)

    def add(self, event):
        self.buffer[event['symbol']].append(event)

    def should_flush(self):
        if not self.buffer:
            return False
        elapsed_time = (datetime.now(tz=timezone.utc) - self.last_flush_time).total_seconds()
        return elapsed_time >= self.flush_interval or any(len(events) >= self.max_size for events in self.buffer.values())

    def flush(self):
        flushed_data = dict(self.buffer)
        self.last_flush_time = datetime.now(tz=timezone.utc)
        self.buffer.clear()
        return flushed_data