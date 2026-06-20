import yfinance as yf
from kafka import KafkaProducer
import json
import time
import os 

TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8')
)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')



def produce_stock_prices():
    while True:
        for ticker in TICKERS:
            try:
                data = yf.ticker(ticker).fast_info

                stock_price = {
                    'ticker': ticker,
                    'price': data['last_price'],
                    'timestamp': time.time(),
                    'high': data['day_high'],
                    'low': data['day_low'],
                    'volume': data['volume'],
                    'currency': data['currency'],
                    'open': data['day_open'],
                    'shares': data['shares'], 
                }

                producer.send('stock_prices', value=stock_price, key=ticker).add_callback(delivery_report)
                producer.flush()
                time.sleep(1)
            
            except Exception as e:
                print(f'Error fetching data for {ticker}: {e}')
                continue

if __name__ == "__main__":
    produce_stock_prices()


