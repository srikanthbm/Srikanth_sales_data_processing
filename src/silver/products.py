from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from pyspark.sql import DataFrame

def clean_products_duplicate(df: DataFrame) -> DataFrame:
    df_cleaned = df.dropDuplicates()
    df_cleaned = df_cleaned.filter(df_cleaned["product_id"].isNotNull())
    df_cleaned = df_cleaned.dropDuplicates(["product_id"])
    
    return df_cleaned

def cast_price(df: DataFrame) -> DataFrame:
    return df.withColumn("price_per_product", F.col("price_per_product").try_cast(DecimalType(10,2)))

