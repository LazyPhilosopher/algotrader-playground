from dataclasses import dataclass
from datetime import datetime

@dataclass
class Stock:
    date: datetime
    high: float
    low: float
    close: float
    volume: float

    def __init__(self, date: str, high: float, low: float, close: float, volume: float):
        self.date = date
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

