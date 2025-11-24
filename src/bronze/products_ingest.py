from pyspark.sql import DataFrame
import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
from bronze.base_ingest import normalize_columns, add_ingestion_metadata
from pyspark.sql import functions as F

def transform_products(df: DataFrame) -> DataFrame:
    df = normalize_columns(df)
    df = add_ingestion_metadata(df)
    return df

 