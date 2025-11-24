
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="function")
def spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark