#%pip install pytest
from pyspark.sql import SparkSession
import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from silver.customers import clean_customers_duplicate, clean_customer_name, clean_phone_number, clean_address

def test_clean_customers_entire_row_duplicates(spark):
    data = [
        Row(customer_id=1, name="Alice"),
        Row(customer_id=1, name="Alice"),
        Row(customer_id=2, name="Bob")
    ]
    df = spark.createDataFrame(data)
    cleaned_df = clean_customers_duplicate(df) 
    result = [ (row.customer_id, row.name) for row in cleaned_df.collect() ]
    
    assert result.count((1, "Alice")) == 1
    assert (2, "Bob") in result

def test_clean_customers_null_and_duplicates(spark):
    data = [
        Row(customer_id=None, name="Unknown1"),
        Row(customer_id=None, name="Unknown1"),  # duplicate with null id
        Row(customer_id=1, name="Alice"),
        Row(customer_id=1, name="Alice Duplicate"),
        Row(customer_id=2, name="Bob")
    ]
    df = spark.createDataFrame(data)
    cleaned_df = clean_customers_duplicate(df)
    result = [row.customer_id for row in cleaned_df.collect()]

    assert None not in result
    assert result.count(1) == 1
    assert 2 in result


def test_clean_customer_name(spark):
    df = spark.createDataFrame(
        [("John.. Cena!!",)],
        ["customer_name"]
    )

    result = clean_customer_name(df).collect()[0]["customer_name"]
    assert result == "John Cena"


def test_clean_phone_basic(spark):
    df = spark.createDataFrame([
        Row(phone="0017185624866"),
        Row(phone="718-562-4866"),
        Row(phone="421.580.0902x9815")
    ])
    cleaned_df = clean_phone_number(df)
    result = [row.phone for row in cleaned_df.collect()]
    assert result[0] == "7185624866"
    assert result[1] == "7185624866"
    assert result[2] == "4215800902x9815"

def test_clean_phone_number_invalid(spark):
    data = [
        Row(phone="abc1234567"),
        Row(phone="-12345"),
        Row(phone="#ERROR!")
    ]
    df = spark.createDataFrame(data)
    cleaned_df = clean_phone_number(df)
    result = [row.phone for row in cleaned_df.collect()]
    assert result[0] is None
    assert result[1] is None
    assert result[2] is None


def test_clean_phone_number_edge_cases(spark):
    data = [
        Row(phone="0015424150246x314"),
        Row(phone="0015424150246"),
        Row(phone="7185624866"),
        Row(phone="4215800902x9815")
    ]
    df = spark.createDataFrame(data)
    cleaned_df = clean_phone_number(df)
    result = [row.phone for row in cleaned_df.collect()]
    assert result[0] == "5424150246x314"  # trimmed first 3 digits
    assert result[1] == "5424150246"      # trimmed first 3 digits
    assert result[2] == "7185624866"
    assert result[3] == "4215800902x9815"

def test_clean_phone_null(spark):
    schema = StructType([StructField("phone", StringType(), True)])
    df = spark.createDataFrame([Row(phone=None)], schema=schema)

    result = clean_phone_number(df).collect()[0]

    assert result.phone is None

def test_clean_address(spark):
    df = spark.createDataFrame([("123 Main Street\nCity, State, 12345",)], ["address"])
    result = clean_address(df).collect()[0]["address"]
    assert result == "123 Main Street, City, State, 12345"

def test_clean_address_null(spark):
    schema = StructType([StructField("address", StringType(), True)])
    df = spark.createDataFrame([Row(address=None)], schema=schema)
    result = clean_address(df).collect()[0]
    assert result.address is None
