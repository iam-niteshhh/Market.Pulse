"""
    This is the Bronze layer, Runs on the Spark, takes in the Data produced by Kafka Producers.
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, DoubleType
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder \
    .appName("Market_Pulse-Bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()


KAFKA_BOOTSTRAP = "kafka:9092"
TOPICS = "fx_rates,reddit_posts,stock_prices"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPICS) \
    .option("startingOffsets", 'latest') \
    .load() \
    .withColumn("ingested_at", current_timestamp())

query = df.writeStream \
    .format('delta') \
    .option('checkpointLocation', "../../data/spark/checkpoints/bronze") \
    .partitionBy('topic') \
    .start('../../data/delta/bronze')


query.awaitTermination()