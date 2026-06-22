import praw
import os
import time
from kafka import KafkaProducer
import json
from textblob import TextBlob


SUBREDDITS = ['stocks', 'investing', 'wallstreetbets']

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8')
)

reddit_collector = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


def produce_reddit_posts():

    subreddit = reddit_collector.subreddit('+'.join(SUBREDDITS))
    for submission in subreddit.stream.submissions(skip_existing=False):
        text = f"{submission.title} {submission.selftext}"
        sentiment = TextBlob(text).sentiment

        record = {
            'id': submission.id,
            'title': submission.title,
            'subreddit': submission.subreddit.display_name,
            'created_utc': submission.created_utc,
            'score': submission.score,
            'sentiment_polarity': sentiment.polarity,
            'sentiment_subjectivity': sentiment.subjectivity,
        }

        producer.send('reddit_posts', value=record, key=submission.subreddit.display_name).add_callback(delivery_report)
        producer.flush()

