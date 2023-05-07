from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, DateType
import yaml

import os
config_file_path = os.path.abspath('../../config/scraper_config.yaml')
with open('config/scraper_config.yaml', 'r') as f:
    config = yaml.safe_load(f)


spark = SparkSession.builder \
    .appName('KafkaSparkStreamingRedditDataVizComments') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', config['kafka']['server']) \
    .option('subscribe', config['kafka']['topic_name']) \
    .option('startingOffsets', 'earliest') \
    .load()

schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("creation_date", StringType()),
    StructField("subject", StringType()),
    StructField("comments", ArrayType(
        StructType([
            StructField("id", StringType()),
            StructField("text", StringType()),
            StructField("creation_date", StringType())
        ])
    ))
])

# Parse the key-value pairs from Kafka into a structured schema
parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("parsed_value"))

# Explode the comments column in the structured schema
exploded_df = parsed_df.select("parsed_value.*").select("id", "title", "creation_date", "subject", explode("comments").alias("comment"))

# Select the columns of interest from the exploded schema
final_df = exploded_df.select("id", "title", "creation_date", "subject", "comment.*")

# Start the streaming query and write the output to the console
query = final_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()
