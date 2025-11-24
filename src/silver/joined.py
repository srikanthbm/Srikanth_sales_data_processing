from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def join_orders_products_customers(orders: DataFrame, products: DataFrame, customers: DataFrame) -> DataFrame:
    
    products = products.drop("state")
    customers= customers.drop("state")
    df = orders.join(products, "product_id", "inner").drop("state")
    df = df.join(customers, "customer_id", "inner")
    df = df.withColumn("profit", F.round(F.col("profit"), 2))
    return df