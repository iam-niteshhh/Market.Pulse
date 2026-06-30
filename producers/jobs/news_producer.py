import os
import time
import json
import logging
import requests

from kafka import KafkaProducer
from textblob import TextBlob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "1f52eb0de96248909d2ea8357ceea2a1")

if not NEWS_API_KEY:
    raise ValueError("NEWS_API_KEY environment variable is not set.")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "market-news"
QUERY = (
    "stock market OR investing OR finance OR "
    "NASDAQ OR NYSE OR Apple OR Microsoft OR Tesla OR Amazon OR Google"
)

NEWS_URL = "https://newsapi.org/v2/everything"

FETCH_INTERVAL = 300  # Fetch every 5 minutes
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    key_serializer=lambda x: x.encode("utf-8"),
)

def delivery_report(metadata_or_error):
    try:
        logger.info(
            f"Message delivered to "
            f"{metadata_or_error.topic}[{metadata_or_error.partition}] "
            f"offset={metadata_or_error.offset}"
        )
    except Exception as e:
        logger.error(f"Delivery failed: {metadata_or_error} | {e}")

def fetch_market_news():
    params = {
        "q": QUERY,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 20,
        "apiKey": NEWS_API_KEY,
    }
    response = requests.get(NEWS_URL, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()
    if data.get("status") != "ok":
        raise Exception(data)
    return data.get("articles", [])

def produce_news():
    logger.info("Starting Market News Producer...")
    seen_articles = set()
    while True:
        try:
            articles = fetch_market_news()
            logger.info(f"Fetched {len(articles)} articles")
            for article in articles:
                article_id = article.get("url")
                if not article_id:
                    continue
                # Avoid duplicates
                if article_id in seen_articles:
                    continue
                seen_articles.add(article_id)
                text = " ".join(
                    filter(
                        None,
                        [
                            article.get("title"),
                            article.get("description"),
                        ],
                    )
                )
                sentiment = TextBlob(text).sentiment
                record = {
                    "id": article_id,
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "content": article.get("content"),
                    "author": article.get("author"),
                    "source": article.get("source", {}).get("name"),
                    "url": article.get("url"),
                    "image_url": article.get("urlToImage"),
                    "published_at": article.get("publishedAt"),
                    "timestamp": time.time(),

                    # Sentiment
                    "sentiment_polarity": sentiment.polarity,
                    "sentiment_subjectivity": sentiment.subjectivity,
                }
                producer.send(
                    TOPIC,
                    key=record["source"] or "news",
                    value=record,
                ).add_callback(delivery_report)
                logger.info(
                    f"[{record['source']}] "
                    f"{record['title']} "
                    f"(Polarity={sentiment.polarity:.2f})"
                )
            producer.flush()
            logger.info(
                f"Sleeping for {FETCH_INTERVAL} seconds..."
            )
            time.sleep(FETCH_INTERVAL)
        except Exception as e:
            logger.exception(f"Producer error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    produce_news()