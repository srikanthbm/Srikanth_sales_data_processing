import sys
sys.path.append('/Workspace/Users/bmsrikanth100@gmail.com/src/')
from silver.joined import join_orders_products_customers


def test_join_orders_products_customers(spark):
    orders = spark.createDataFrame(
        [(1, 10, 100.554)], ["customer_id", "product_id", "profit"]
    )
    products = spark.createDataFrame(
        [(10, "Cat", "Sub")], ["product_id", "category", "sub_category"]
    )
    customers = spark.createDataFrame(
        [(1, "John", "IN")], ["customer_id", "customer_name", "country"]
    )

    df = join_orders_products_customers(orders, products, customers)
    assert df.count() == 1
    assert df.first().profit == 100.55
