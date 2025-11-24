from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

def normalize_columns(df):
    return df.toDF(*[
        c.lower().strip().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ])

def add_ingestion_metadata(df):
    return df.withColumn("ingestion_timestamp", F.current_timestamp())