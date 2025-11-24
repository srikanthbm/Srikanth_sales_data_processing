import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
#%pip install pytest
import pytest
from pyspark.sql import Row
from gold.gold_aggregate import add_year_column, aggregate_profit


def test_add_year_column(spark):
    df = spark.createDataFrame(
        [
            ("2022-05-10", 100.50),
            ("2021-12-01", 200.10)
        ],
        ["order_date", "profit"]
    )

    result = add_year_column(df).collect()

    assert result[0]["year"] == 2022
    assert result[1]["year"] == 2021


def test_aggregate_profit_basic(spark):
    df = spark.createDataFrame(
        [
        Row(year=2020, category="Furniture", sub_category="Chairs", customer_id="C1", profit=100.13),
        Row(year=2020, category="Furniture", sub_category="Chairs", customer_id="C1", profit=200.16),
        Row(year=2020, category="Office Supplies", sub_category="Paper", customer_id="C2", profit=50.79),
        Row(year=2021, category="Furniture", sub_category="Tables", customer_id="C3", profit=300.79),
    ]
    )

    aggregated = aggregate_profit(df)
    result = [(row.year, row.category, row.sub_category, row.customer_id, row.total_profit) 
              for row in aggregated.collect()]
 

    assert (2020, "Furniture", "Chairs", "C1", 300.29) in result # 100.13 + 200.16
    assert (2020, "Office Supplies", "Paper", "C2", 50.79) in result
    assert (2021, "Furniture", "Tables", "C3", 300.79) in result


def test_aggregate_profit_rounding(spark):
    df = spark.createDataFrame([
        Row(order_date="2022-01-01", category="Tech", sub_category="Audio", customer_id="C33", profit=10.129),
        Row(order_date="2022-01-02", category="Tech", sub_category="Audio", customer_id="C33", profit=0.127)
    ])

    df = add_year_column(df)
    aggregated = aggregate_profit(df)
    result = aggregated.collect()[0]

    assert result["total_profit"] == 10.26   # 10.129 + 0.127 = 10.256 → rounded to 10.26
