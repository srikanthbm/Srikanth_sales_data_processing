# Databricks notebook source
from pyspark.sql import SparkSession
import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src')
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from bronze.products_ingest import transform_products

def func_run(input_path: str, output_table: str):
    try:
        spark = SparkSession.builder.appName("Products_Raw_data_Ingestion").getOrCreate()
        product_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("price_per_product", DoubleType(), True)
        ])
        products_raw = (
        spark.read.format("csv").option("header", True).option("quote", "\"").option("escape", "\"")
        .schema(product_schema).load(input_path))

        transformed_df = transform_products(products_raw)

        (transformed_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(output_table))
        print(f"ingestion completed")
    except Exception as e:
        print(f"Error in func_run: {e}")
        return None

func_run(
    input_path="dbfs:/Volumes/workspace/bronze/raw_data/Products.csv",
    output_table="workspace.bronze.products_raw"
)


# COMMAND ----------

df=spark.read.table("workspace.bronze.products_raw")
display(df.head(50))
