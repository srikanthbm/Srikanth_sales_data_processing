# Databricks notebook source
# DBTITLE 1,Orders -- read-transform-write
import sys
sys.path.append("/Workspace/Users/bmsrikanth100@gmail.com/src")
from pyspark.sql import SparkSession
from silver.orders import clean_order_dates,clean_orders_duplicate


orders_bronze = spark.read.table("workspace.bronze.orders_raw")

orders_cleaned = clean_orders_duplicate(orders_bronze)
orders_cleaned = clean_order_dates(orders_cleaned)
#orders_cleaned.printSchema()

orders_cleaned.write.format("delta").option("overwriteSchema","true").mode("overwrite").saveAsTable("workspace.silver.enriched_orders")

print("Silver Orders Transformation Completed Successfully!")

df = spark.read.table("workspace.silver.enriched_orders")
display(df.limit(10))



# COMMAND ----------

