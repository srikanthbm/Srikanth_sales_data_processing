# Databricks notebook source
# DBTITLE 1,Customers -- read-transform-write
import sys
sys.path.append("/Workspace/Users/bmsrikanth100@gmail.com/src")
from silver.customers import clean_customer_name, clean_phone_number, clean_address, clean_customers_duplicate

df = spark.read.table("workspace.bronze.customers_raw")

df = clean_customers_duplicate(df)
df = clean_customer_name(df)
print("clean_customer_name")
df = clean_phone_number(df)
print("clean_phone_number")
df = clean_address(df)
display(df.head(5))
print("clean_address")

df.printSchema()
df.write.format("delta").option("overwriteSchema","true").mode("overwrite").saveAsTable("workspace.silver.enriched_customers")
spark.read.table("workspace.silver.enriched_customers").display()


# COMMAND ----------

