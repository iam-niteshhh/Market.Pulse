"""
    This is the Spark Gold layers, inteded to do windowed data aggregation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, expr, window



spark = SparkSession.builder \
    .appName("Market_Pulse-Gold") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()


df_silver_stocks = spark.readStream \
    .format("delta") \
    .load("../../data/delta/silver/stocks")


df_silver_reddit = spark.readStream \
    .format("delta") \
    .load("../../data/delta/silver/reddit_posts")


df_silver_fx_rates = spark.readStream \
    .format("delta") \
    .load("../../data/delta/silver/fx_rates")


# 5-minute tumbling window on stock prices
stock_windowed = df_silver_stocks \
    .groupBy(
        window(col('event_time'), '5 minutes'),
        col('ticker')
    ) \
    .agg(
        avg('close').alias('avg_close'),
        avg('volume').alias('avg_volume')
    )

sentiment_windowed = df_silver_reddit \
    .groupBy(
        window(col('event_time'), '5 minutes')
    ) \
    .agg(
        avg('sentiment_polarity').alias('avg_sentiment'),
        avg('sentiment_subjectivity').alias('avg_subjectivity')
    )


# fx_rates_windowed = df_silver_fx_rates \
#     .groupBy(
#         window(col('event_time'), '5 minutes'),
#         col('base'),
#         col('target')
#     ) \
#     .agg(
#         avg('rate').alias('avg_rate')
#     )

gold_df = stock_windowed.join(
    sentiment_windowed,
    on='window',
    how='left'
)

gold_df.writeStream \
    .format('delta') \
    .option('checkpointLocation', '../../data/checkpoints/gold') \
    .outputMode('append') \
    .start('../../data/delta/gold')