from pyspark.sql import SparkSession




def build_spark_session():
    return (
        SparkSession.builder
        .appName("Register-Silver-Tables")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )

spark = build_spark_session()

spark.sql("""
CREATE DATABASE IF NOT EXISTS market_pulse
""")

tables = [
    "stocks",
    "news_posts",
    "fx_rates"
]

for table in tables:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS market_pulse.{table}
        USING DELTA
        LOCATION '/Users/niteshsaini/Documents/SkillUp/Data_Engineering/Market.Pulse/data/delta/silver/{table}'
    """)
print("DATABASES")
spark.sql("SHOW DATABASES").show()

print("TABLES")
spark.sql("SHOW TABLES IN market_pulse").show()

spark.stop()