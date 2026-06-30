"""
    This is the Silver layer, read the bronze layer, add watermark and save it.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType, DoubleType, LongType, StructField
from pyspark.sql.functions import col, from_json, expr


stock_schema = StructType([
    StructField('ticker', StringType()),
    StructField('open', DoubleType()),
    StructField('high', DoubleType()),
    StructField('low', DoubleType()),
    StructField('close', DoubleType()),
    StructField('volume', LongType()),
    StructField('timestamp', LongType()),
])

reddit_schema = StructType([
    StructField('id', StringType()),
    StructField('title', StringType()),
    StructField('subreddit', StringType()),
    StructField('score', DoubleType()),
    StructField('sentiment_polarity', DoubleType()),
    StructField('sentiment_subjectivity', DoubleType()),
    StructField('timestamp', LongType()),
])

fx_rate_schema = StructType([
    StructField('base', StringType()),
    StructField('target', StringType()),
    StructField('rate', DoubleType()),
    StructField('timestamp', LongType()),
])


def build_spark_session():
    return SparkSession.builder \
        .appName("Market_Pulse-Silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
        .getOrCreate()


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df_bronze = spark.readStream \
        .format("delta") \
        .load("./data/delta/bronze")

    df_stock = df_bronze \
        .filter(col('topic') == "stock-prices") \
        .select(from_json(col('value').cast('string'), stock_schema).alias('data')) \
        .select('data.*') \
        .withColumn('event_time', expr("timestamp_millis(timestamp)")) \
        .withWatermark('event_time', '2 minutes') \
        .dropDuplicates(['ticker', 'timestamp'])

    df_market_news = df_bronze \
        .filter(col('topic') == "market-news") \
        .select(from_json(col('value').cast('string'), reddit_schema).alias('data')) \
        .select('data.*') \
        .withColumn('event_time', expr("timestamp_millis(timestamp)")) \
        .withWatermark('event_time', '2 minutes') \
        .dropDuplicates(['id', 'timestamp'])

    df_fx_rates = df_bronze \
        .filter(col('topic') == "fx-rates") \
        .select(from_json(col('value').cast('string'), fx_rate_schema).alias('data')) \
        .select('data.*') \
        .withColumn('event_time', expr("timestamp_millis(timestamp)")) \
        .withWatermark('event_time', '2 minutes') \
        .dropDuplicates([ 'base', 'target', 'timestamp'])

    query_stock = df_stock.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./data/spark/checkpoints/silver/stocks") \
        .start("./data/delta/silver/stocks")

    query_reddit = df_market_news.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./data/spark/checkpoints/silver/marker_news") \
        .start("./data/delta/silver/reddit_posts")

    query_fx_rates = df_fx_rates.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./data/spark/checkpoints/silver/fx_rates") \
        .start("./data/delta/silver/fx_rates")

    try:
        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()