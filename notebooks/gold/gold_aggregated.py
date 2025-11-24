# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from gold.gold_aggregate import add_year_column , aggregate_profit, 


spark = SparkSession.builder.appName("Aggregated_gold_table").getOrCreate()

df=spark.read.table("workspace.silver.enriched_joined_table")

df=add_year_column(df)
profit_year_cat_subcat_cust=aggregate_profit(df)

display(profit_year_cat_subcat_cust.head(10)) 

profit_year_cat_subcat_cust.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.master")


# COMMAND ----------

# DBTITLE 1,profit by year


df.createOrReplaceTempView(f"gold_aggregate_final")

profit_year=spark.sql(""" select year,round(sum(profit),2) as total_profit_year from gold_aggregate_final group by year order by year """)
profit_year.show()

# COMMAND ----------

# DBTITLE 1,profit by year + category
profit_year_category=spark.sql(""" select year,category,round(sum(profit),2)as total_profit_year_category from gold_aggregate_final group by year,category order by year""")
profit_year_category.show()

# COMMAND ----------

# DBTITLE 1,profit by customer
profit_customer=spark.sql(""" select customer_id,round(sum(profit),2) as total_profit_customer from gold_aggregate_final group by customer_id order by total_profit_customer desc""") 

profit_customer.show()

# COMMAND ----------

# DBTITLE 1,profit by customer +  year
profit_customer_year=spark.sql(""" select customer_id,year,round(sum(profit),2) as total_profit_customer_year from gold_aggregate_final group by customer_id,year order by year""")
profit_customer_year.show()

# COMMAND ----------

