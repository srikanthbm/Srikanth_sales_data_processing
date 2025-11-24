
import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
from silver.products import cast_price, clean_products_duplicate
from pyspark.sql import Row


def test_clean_products_duplicate(spark):
    df = spark.createDataFrame([
        Row(product_id="P1", category="Furniture", sub_category="Chair"),
        Row(product_id="P1", category="Furniture", sub_category="Chair"),
        Row(product_id=None, category="Furniture", sub_category="Table"), 
        Row(product_id="P2", category="Furniture", sub_category="Table")
    ])

    cleaned_df = clean_products_duplicate(df)
    result = [row.product_id for row in cleaned_df.collect()]

    assert None not in result
    assert result.count("P1") == 1
    assert result.count("P2") == 1

def test_cast_price(spark):
    df = spark.createDataFrame(
        [(1, "10")],
        "product_id int, price_per_product string"
    )
    
    result = cast_price(df)
    assert result.schema["price_per_product"].dataType.simpleString() == "decimal(10,2)"
