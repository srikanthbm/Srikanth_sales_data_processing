# Databricks notebook source
# Databricks Python script for Silver – Products Transformation
import sys
sys.path.append("/Workspace/Users/bmsrikanth100@gmail.com/src")
from pyspark.sql import SparkSession
from silver.products import cast_price, clean_products_duplicate
from silver.utils import save_delta

products_bronze = spark.read.table("workspace.bronze.products_raw")

products_cleaned = clean_products_duplicate(products_bronze)
products_cleaned = cast_price(products_cleaned)

#products_cleaned.printSchema()
#display(products_cleaned)
products_cleaned.write.format("delta").option("overwriteSchema","true").mode("overwrite").saveAsTable("workspace.silver.enriched_products")

spark.read.table("workspace.silver.enriched_products").display()

print("Silver Products Transformation Completed Successfully!")



# COMMAND ----------

spark.read.table("workspace.silver.enriched_products").display()

# COMMAND ----------

