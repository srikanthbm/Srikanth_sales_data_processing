# Databricks notebook source
# Databricks Python script for Silver – Enriched Joined Table
import sys
sys.path.append("/Workspace/Users/bmsrikanth100@gmail.com/src")
from pyspark.sql import SparkSession
from silver.joined import join_orders_products_customers
from pyspark.sql import functions as F


def run_enriched_joined_silver():
    spark = SparkSession.builder.getOrCreate()


    enriched_orders = spark.read.table("workspace.silver.enriched_orders")
    enriched_orders = enriched_orders.drop("state")

    enriched_products = spark.read.table("workspace.silver.enriched_products")

    enriched_customers = spark.read.table("workspace.silver.enriched_customers")
    enriched_customers=enriched_customers.drop(F.col("state"))

    print(" Performing Silver Joins (Orders + Products + Customers)...")

    enriched_joined = join_orders_products_customers(
        enriched_orders, enriched_products, enriched_customers
    )

    print("Writing Joined Silver Output...")
    enriched_joined = enriched_joined.select("customer_id","product_id","discount","order_date","order_id","price","profit","quantity","row_id","ship_date","ship_mode","category","sub_category","customer_name","country")

    enriched_joined.write.format("delta").option("overwriteSchema","true").mode("overwrite").saveAsTable("workspace.silver.enriched_joined_table")

    print("Silver Enriched Joined Table Successfully Completed!")


run_enriched_joined_silver()


# COMMAND ----------

spark.read.table("workspace.silver.enriched_joined_table").display()

# COMMAND ----------

