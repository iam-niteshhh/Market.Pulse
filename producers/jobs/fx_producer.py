import time
import os
import json
from kafka import KafkaProducer
import requests

FX_URL = f'https://api.frankfurter.dev/v2/rates'

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

kafka_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8')
)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


def produce_fx_rates(base_currency='USD'):
    while True:
        try:
            response = requests.get(FX_URL, params={'base': base_currency})
            response.raise_for_status()
            data = response.json()

            for currency in data:
                record = {
                    'timestamp': time.time(),
                    'base': currency['base'],
                    'target': currency['quote'],
                    'rate': currency['rate']
                }

                kafka_producer.send('fx_rates', value=record, key=currency['base']).add_callback(delivery_report)
                kafka_producer.flush()
                time.sleep(60)  # Fetch rates every minute

        except Exception as e:
            print(f'Error fetching FX rates: {e}')
            time.sleep(60)  # Wait before retrying
            