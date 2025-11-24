import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
from bronze.customers_ingest import transform_customers
from pyspark.sql import Row

def test_customers_ingest(spark):
    rows = [
        Row(Customer_ID="C1", Customer_Name="John Cena")
    ]
    df = spark.createDataFrame(rows)
    result = transform_customers(df).collect()[0]

    assert "customer_id" in result.asDict()
    assert result["customer_id"] == "C1"

def test_ingestion_metadata_added(spark):

    data = [("Alice", "C3"), ("Bob", "C2")]
    columns = ["customer_name", "customer_id"]
    df = spark.createDataFrame(data, columns)
    result = transform_customers(df)
    
    assert "ingestion_timestamp" in result.columns

def test_duplicate_records_preserved(spark):
    df = spark.createDataFrame([Row(customer_id="1"), Row(customer_id="1")])
    original_count = df.count()
    result_df = transform_customers(df)
    transformed_count = result_df.count()

    assert transformed_count == original_count