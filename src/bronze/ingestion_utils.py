from pyspark.sql import SparkSession

def get_spark(app_name: str = "Bronze_Ingestion"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_data(spark, input_path: str, file_type: str = "csv", **options):
    if file_type == "csv":
        return spark.read.format("csv").options(**options).load(input_path)
    elif file_type == "json":
        return spark.read.format("json").options(**options).load(input_path)
    elif file_type in ["xls", "xlsx", "excel"]:
        import pandas as pd
        pandas_df = pd.read_excel(input_path)
        return spark.createDataFrame(pandas_df)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

def write_delta(df, output_table: str, mode="overwrite", merge_schema=True):
    df.write.format("delta") \
        .mode(mode) \
        .option("mergeSchema", merge_schema) \
        .saveAsTable(output_table)
