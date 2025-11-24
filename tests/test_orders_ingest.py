import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
from bronze.orders_ingest import transform_orders
from pyspark.sql import Row
from datetime import date



def test_orders_ingest(spark):
    rows = [
        Row(Order_Date="21/8/2016", Ship_Date="25/8/2016")
    ]
    df = spark.createDataFrame(rows)

    result = transform_orders(df).collect()[0]

    # Check new standardized columns
    assert "order_date" in result.asDict()
    assert "ship_date" in result.asDict()


def test_ingestion_metadata_added(spark):

  data = [("1", "1/11/2025", "3/11/2025")]
  columns = ["order_id", "order_date", "ship_date"]
  df = spark.createDataFrame(data, columns)
    
  result = transform_orders(df)
    
  assert "ingestion_timestamp" in result.columns


def test_duplicate_records_preserved(spark):
    data = [("1", "1/11/2025", "3/11/2025"),("1", "1/11/2025", "3/11/2025")]
    columns = ["order_id", "order_date", "ship_date"]
    df = spark.createDataFrame(data,columns)
    original_count = df.count()
    result_df = transform_orders(df)
    transformed_count = result_df.count()

    assert transformed_count == original_count


