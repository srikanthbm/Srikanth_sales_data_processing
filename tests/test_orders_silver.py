import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
from silver.orders import clean_orders_duplicate, clean_order_dates
from pyspark.sql.types import Row

def test_clean_orders_full_row_duplicate(spark):
    df = spark.createDataFrame([
        Row(Order_ID="CA-2016-122581", Product_ID="FUR-CH-10002961", Quantity=7),
        Row(Order_ID="CA-2016-122581", Product_ID="FUR-CH-10002961", Quantity=7)
    ])
    
    cleaned_df = clean_orders_duplicate(df)
    assert cleaned_df.count() == 1


def test_clean_orders_key_null_or_duplicate(spark):
    df = spark.createDataFrame([
        Row(Order_ID="CA-2016-122581", Product_ID="FUR-CH-10002961", Quantity=7),
        Row(Order_ID=None, Product_ID="OFF-BI-10000851", Quantity=3), 
        Row(Order_ID="CA-2016-122581", Product_ID=None, Quantity=5), 
        Row(Order_ID="CA-2016-122581", Product_ID="FUR-CH-10002961", Quantity=2),
        Row(Order_ID="CA-2016-122582", Product_ID="OFF-BI-10000851", Quantity=1)
    ])
    cleaned_df = clean_orders_duplicate(df)
    key = [(row["Order_ID"], row["Product_ID"]) for row in cleaned_df.collect()]
    
    assert (None, "OFF-BI-10000851") not in key
    assert ("CA-2016-122581", None) not in key
    
    assert key.count(("CA-2016-122581", "FUR-CH-10002961")) == 1


def test_clean_order_dates(spark):
    df = spark.createDataFrame(
        [("1/2/2020", "5/2/2020")],
        ["order_date", "ship_date"]
    )
    result = clean_order_dates(df).first()
    assert str(result.order_date) == "2020-02-01"
    assert str(result.ship_date) == "2020-02-05"
