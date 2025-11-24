
import pytest
import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
from bronze.products_ingest import transform_products
from pyspark.sql import Row

def test_products_ingest(spark):
    rows = [Row(Product_ID="P1", Price_per_product="10.55")]
    df = spark.createDataFrame(rows)

    result = transform_products(df).collect()[0]

    # Check column normalization
    assert "product_id" in result.asDict()
    assert "price_per_product" in result.asDict()

def test_ingestion_metadata_added(spark):

    data = [(100,)]
    columns = ["price_per_product" ]
    df = spark.createDataFrame(data, columns)
    
    result = transform_products(df)
    
    assert "ingestion_timestamp" in result.columns

def test_duplicate_records_preserved(spark):
    df = spark.createDataFrame([Row(product_id="1", price_per_product="10.5"),
                                Row(product_id="1", price_per_product="10.5")])
    original_count = df.count()
    result_df = transform_products(df)
    transformed_count = result_df.count()

    assert transformed_count == original_count
