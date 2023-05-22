from pyspark.sql.types import StructType, StructField, StringType

def get_schema(schema_name):
    if schema_name == "comment":
        return StructType([
            StructField("comment_id", StringType()),
            StructField("comment_text", StringType()),
            StructField("comment_creation_date", StringType()),
            StructField("comment_subject", StringType())
        ])
    elif schema_name == "post":
        return StructType([
            StructField("post_id", StringType()),
            StructField("post_title", StringType()),
            StructField("post_creation_date", StringType()),
            StructField("post_subject", StringType())
        ])
    else:
        raise ValueError("Invalid schema name")