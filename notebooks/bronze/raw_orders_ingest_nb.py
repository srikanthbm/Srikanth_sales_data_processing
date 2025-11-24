# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
from bronze.orders_ingest import transform_orders

def func_load(input_path: str , output_table: str):
    try:
        spark = SparkSession.builder.appName("Orders_Raw_data_Ingestion").getOrCreate()
        orders_raw = (spark.read.format("json")
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .option("multiLine", True)
        .load(input_path))
        
        transformed_orders_df=transform_orders(orders_raw)

        transformed_orders_df.write.format("delta").option("mergeSchema","true").mode("overWrite").saveAsTable(output_table)
        print(f"ingestion Completed")
        return transformed_orders_df
    except Exception as e:
        print(f"Error in func_load: {e}")
        return None

func_load(input_path ="dbfs:/Volumes/workspace/bronze/raw_data/Orders.json", output_table= "workspace.bronze.orders_raw" )


# COMMAND ----------

df=spark.read.table("workspace.bronze.products_raw")
display(df.head(4))

# COMMAND ----------

