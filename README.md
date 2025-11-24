# Medallion Architecture Data Engineering Project (Bronze → Silver → Gold)
 E-commerce Sales Data Processing with Databricks.

 This project implements a complete end-to-end data engineering pipeline using the Medallion Architecture (Bronze, Silver, Gold) in Databricks. It is fully modular, production-ready, and supported by extensive unit testing using pytest.

The goal of this project is to demonstrate clean, scalable, and testable Data Engineering best practices using PySpark, Delta Lake, and Databricks.

**Scenario: E-commerce Sales Data Processing with Databricks**
You've been assigned to design and implement a data processing system using Databricks for an e-commerce platform. This platform generates a lot of sales data, including details about orders, products, customers, and transactions. Your goal is to use Databricks to create a scalable, efficient, and reliable data engineering solution. This solution should process and analyze the data to provide valuable insights for business stakeholders.


**Source Datasets:**
You are required to download the datasets from the below drive
https://drive.google.com/drive/folders/1eWxfGcFwJJKAK0Nj4zZeCVx6gagPEEVc?usp=sharing


**Data Transformation and Processing:**
Your task is to process the raw sales data using Databricks notebooks and PySpark. You need to clean up the data and transform it into structured formats suitable for analysis. Specifically, you should create a master table and perform aggregations based on the requirements provided.
Note: Write appropriate Test Cases (Unit Tests) to ensure the correctness for the given scenarios. Use PySpark (not SQL) for this task.  			

**Task**
1. Create raw tables for each source dataset
2. Create an enriched table for customers and products 
3. Create an enriched table which has
  a. order information 
    > Profit rounded to 2 decimal places
  c. Customer name and country
  d. Product category and sub category
4. Create an aggregate table that shows profit by 
  a. Year
  b. Product Category
  c. Product Sub Category
  d. Customer

Using SQL output the following aggregates
1. Profit by Year
2. Profit by Year + Product Category
3. Profit by Customer
4. Profit by Customer + Year

Notes:
> Ensure you understand the task requirements thoroughly before starting.
> Pay attention to specific details and expectations outlined in the task descriptions.
> Use a test-driven development approach to validate the correctness of your implementations.
> Write comprehensive test cases to cover different scenarios and edge cases.
> Ensure your solution handles data quality issues and implements robust error-handling mechanisms.
> Document your code and assumptions clearly to make it understandable for others.
> Consider performance implications and optimize your code for efficiency and scalability.

## Overview
This project ingests and validates raw **orders**, **products**, and **customers**
datasets using PySpark. Data is normalized, validated, and transformed into
clean bronze/silver tables in a Delta Lake environment.

## Assumptions
- customer_id, order_id and product_id are treated as primary keys for their respective datasets.
- All numeric fields like profit and price need to be rounded to 2 decimal places.
- The system is designed to scale with large datasets using Spark transformations.
- product Prices must be non-negative

## Bronze Layer – Raw Ingestion

The Bronze layer stores raw JSON/CSV files and metadata exactly as received from source systems.
It performs no transformations, only ingestion and normalization of column names.

Features:

- Ingests raw Customer, Product, and Order datasets
- Normalize column names to lowercase with underscores.
- Add ingestion metadata (ingestion_timestamp) for traceability.
- Landing zone ingestion using Delta format
- Prepared for downstream cleansing

## Silver Layer – Cleansing & Enrichment

The Silver layer applies all business-rule transformations and data cleansing.

Key Silver Transformations:

- Customer cleansing
  1. Remove duplicate rows and ensure no null values for primary key customer_id
  2. Remove special characters
  3. Normalize spacing
  4. Clean phone numbers

- Product enrichment
  1. Standardize product prices and round to 2 decimal places
  2.  Remove duplicate rows and ensure no null values for primary key product_id
     
- Order enrichment
  1. Convert date formats
  2. Remove duplicate rows and ensure no null values for primary key order_id

- Joined Silver Table
  1. Join Orders + Products + Customers
  2. Round profit to 2 decimals
  3. Enrich order data with customer and product details

All transformations are implemented as reusable PySpark functions (pure functions) with clear separation of logic.

## Gold Layer – Aggregated Data for Analytics

The Gold layer prepares business-ready aggregated datasets for analytics and dashboarding.

Gold Aggregations:
- Extract year from order date
- Aggregate profit by:
   - year
   - category
   - sub-category
   - customer
- Perform aggregate views for analytics such as:
  -Profit by Year
  -Profit by Year + Product Category
  -Profit by Customer
  -Profit by Customer + Year
  
- Apply business rounding
- Write optimized Delta tables for BI consumption
## Unit Testing with Pytest

Every transformation in Bronze, Silver, and Gold is thoroughly tested.
Tests ensure correctness, reliability, and isolation of logic.

Pytest Features:

- Spark session fixture (conftest.py)
- Tests for:
   - Test for duplicate removal, null handling, and proper rounding of numeric fields
   - customer cleansing
   - phone number formatting
   - date parsing
   - data type conversions
   - join logic
   - gold aggregation and rounding
- Validates schema, data values, and edge cases
- Designed to run in CI/CD pipeline
- Run tests using test_runner/run_all_tests.pynb.
