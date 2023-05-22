from pyspark.sql.functions import *
from pyspark.sql.types import *


def load_to_s3(df, s3_path):
    # Define the S3 output path
    s3_output_path = 's3a://' + s3_path
    # Start the streaming query and write the output to S3
    query = df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("checkpointLocation", 'reddit-data-viz-comments/checkpoints') \
        .option("path", s3_output_path) \
        .start()

    # Wait for the streaming query to terminate
    query.awaitTermination()

if __name__ == '__main__':
    load_to_s3()