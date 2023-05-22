from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import lit

def consume_from_kafka(spark_session, kafka_server, comments_topic, posts_topic):
    schema= StructType([
            StructField("id", StringType()),
            StructField("text", StringType()),
            StructField("creation_date", StringType()),
            StructField("subject", StringType()),
            StructField("type", StringType()),
            StructField("sentiment_score", DoubleType())
        ])

    df = spark_session \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_server) \
        .option('subscribe', f'{comments_topic}, {posts_topic}') \
        .option('startingOffsets', 'latest') \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse the key-value pairs from Kafka into a structured schema
    parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("parsed_value"))

    # Explode the comments column in the structured schema
    final_df = parsed_df.select("parsed_value.*").select("id", "text", "creation_date", "subject", "type", "sentiment_score")

    return final_df

if __name__ == '__main__':
    consume_from_kafka()