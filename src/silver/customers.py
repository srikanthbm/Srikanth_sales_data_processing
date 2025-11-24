from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql import Row

def clean_customers_duplicate(df: DataFrame) -> DataFrame: 
    df_cleaned = df.dropDuplicates()
    df_cleaned = df_cleaned.filter(F.col("customer_id").isNotNull())
    df_cleaned = df_cleaned.dropDuplicates(["customer_id"])
    return df_cleaned

def clean_customer_name(df: DataFrame) -> DataFrame:
    cleaned = (
        df.withColumn("customer_name", F.regexp_replace("customer_name", r"[^A-Za-z\s]", ""))
          .withColumn("customer_name", F.regexp_replace("customer_name", r"\s+", " "))
          .withColumn("customer_name", F.trim("customer_name"))
    )
    return cleaned


def clean_phone_number(df: DataFrame) -> DataFrame:
    
    customers_cleaned_number = df.withColumn(
    "phone",
    F.when(
        (F.col("phone").isNotNull()) & 
        (F.regexp_replace(F.col("phone"), r"[^0-9xX]", "").rlike(r"^\d{10,}(x\d+)?$")),
        F.regexp_replace(F.col("phone"), r"[^0-9xX]", "")  # keep valid pattern
    ).otherwise(None)  # invalid → NULL
)
    customers_cleaned_number_new = customers_cleaned_number.withColumn(
    "phone",
    F.when(
     (F.col("phone").substr(1,3) == "001"),
        F.substring(F.col("phone"), 4, 100)  # remove first 3 digits prefix and storing only 10 digit numbers
    ).otherwise(F.col("phone")) 
)

    return customers_cleaned_number_new

def clean_address(df: DataFrame) -> DataFrame:
    df = df.withColumn("address", F.regexp_replace("address", "\n", ", "))
    return df
