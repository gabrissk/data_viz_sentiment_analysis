from pyspark.sql import SparkSession
import os
import yaml
from extract.kafka_consumer import consume_from_kafka
from transform.sentiment_analysis import do_sentiment_analysis
from load.s3_file_loading import load_to_s3

def run_etl():
    # Load configuration from YAML file
    config_file_path = os.path.abspath('config/config.yaml')
    with open(config_file_path, 'r') as f:
        config = yaml.safe_load(f)

    # Create the SparkSession object
    spark = SparkSession.builder \
        .appName('KafkaSparkStreamingRedditDataViz') \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", config['s3']['access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['s3']['secret_key']) \
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "true") \
        .getOrCreate()

    # Consume data from Kafka
    kafka_df = consume_from_kafka(spark, config['kafka']['server'], config['kafka']['comments_topic'],
                                  config['kafka']['posts_topic'])
    
    # transformed_df = do_sentiment_analysis(kafka_df)
    
    load_to_s3(kafka_df, config['s3']['output_path'])
    

if __name__ == '__main__':
    run_etl()