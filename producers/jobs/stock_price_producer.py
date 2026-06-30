import requests
import time
import json
import os
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("TWELVE_DATA_API_KEY","6f78d48c4496455b9f916d7ca26eb3f7")

# TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
TICKERS = ["GOOGL"]

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    key_serializer=lambda x: x.encode("utf-8")
)

def get_stock_data(symbol):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1min",
        "outputsize": 1,
        "apikey": API_KEY
    }

    r = requests.get(url, params=params)
    data = r.json()

    if "values" not in data:
        raise Exception(f"API error: {data}")

    latest = data["values"][0]

    return {
        "ticker": symbol,
        "price": float(latest["close"]),
        "open": float(latest["open"]),
        "high": float(latest["high"]),
        "low": float(latest["low"]),
        "volume": float(latest["volume"]),
        "timestamp": time.time(),
        "currency": "USD"
    }

def produce():
    while True:
        for ticker in TICKERS:
            try:
                record = get_stock_data(ticker)

                producer.send(
                    "stock-prices",
                    key=ticker,
                    value=record
                )

                logger.info(f"Sent {ticker}: {record}")

                time.sleep(60)

            except Exception as e:
                logger.error(f"{ticker} failed: {e}")

        producer.flush()
        time.sleep(10)

if __name__ == "__main__":
    produce()