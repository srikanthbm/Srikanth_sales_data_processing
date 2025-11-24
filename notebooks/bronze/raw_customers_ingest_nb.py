# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

from pyspark.sql import SparkSession
import sys
import pandas as pd
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
from bronze.customers_ingest import transform_customers

def func_run(input_path: str, output_table: str):
    try:
        spark = (SparkSession.builder.appName("Customers_Raw_Ingestion").getOrCreate())
        # Load Excel using Pandas
        pandas_df = pd.read_excel(input_path)
        pandas_df["phone"] = pandas_df["phone"].astype("string")
        raw_df = spark.createDataFrame(pandas_df)
        transformed_df = transform_customers(raw_df)

        transformed_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(output_table)
        print(f"Ingestion Completed")
        return transformed_df
    except Exception as e:
        print(f"Error in func_run: {e}")
        return None

func_run(
        input_path="/Volumes/workspace/bronze/raw_data/Customer.xlsx",
        output_table="workspace.bronze.customers_raw"
        )


# COMMAND ----------

