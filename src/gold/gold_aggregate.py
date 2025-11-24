from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_year_column(df: DataFrame) -> DataFrame:
    """Extract year from order_date."""
    return df.withColumn("year", F.year(F.col("order_date")))


def aggregate_profit(df: DataFrame) -> DataFrame:
    """
    Group by year, category, sub-category, and customer,
    then compute total profit.
    """
    return (
        df.groupBy("year", "category", "sub_category", "customer_id")
          .agg(F.round(F.sum("profit"), 2).alias("total_profit"))
          .orderBy("year")
    )


