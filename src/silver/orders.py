from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date


from pyspark.sql import DataFrame

def clean_orders_duplicate(df: DataFrame) -> DataFrame:
    df_cleaned = df.dropDuplicates()
    df_cleaned = df_cleaned.filter(
        (df_cleaned["order_id"].isNotNull()) & (df_cleaned["product_id"].isNotNull())
    )
    df_cleaned = df_cleaned.dropDuplicates(["order_id", "product_id"])
    
    return df_cleaned


def clean_order_dates(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "order_date", to_date(F.col("order_date"), "d/M/yyyy")
    ).withColumn(
        "ship_date", to_date(F.col("ship_date"), "d/M/yyyy")
    )
