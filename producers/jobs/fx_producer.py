import time
import os
import json
import logging
from kafka import KafkaProducer
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

FX_URL = f'https://api.frankfurter.dev/v2/rates'

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

kafka_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8')
)


def delivery_report(metadata_or_error):
    try:
        logger.info(
            f"Message delivered to {metadata_or_error.topic} "
            f"[{metadata_or_error.partition}] at offset {metadata_or_error.offset}"
        )
    except Exception as e:
        logger.error(f"Delivery failed: {metadata_or_error} | {e}")

def produce_fx_rates(base_currency='USD'):
    logger.info(f'Starting FX rates producer for base currency: {base_currency}')
    while True:
        try:
            logger.debug(f'Fetching FX rates from {FX_URL} with base currency: {base_currency}')
            response = requests.get(FX_URL, params={'base': base_currency})
            response.raise_for_status()
            data = response.json()
            logger.info(f'Successfully fetched FX rates')
            print("data",data)
            for currency in data:
                record = {
                    'timestamp': time.time(),
                    'base': currency['base'],
                    'target': currency['quote'],
                    'rate': currency['rate']
                }
                print(record)

                kafka_producer.send('fx-rates', value=record, key=currency['base']).add_callback(delivery_report)
                kafka_producer.flush()
            time.sleep(60)  # Fetch rates every minute

        except Exception as e:
            logger.error(f'Error fetching FX rates: {e}', exc_info=True)
            time.sleep(60)  # Wait before retrying

if __name__ == "__main__":
    produce_fx_rates()