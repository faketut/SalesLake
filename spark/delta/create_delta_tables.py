# Delta Lake table creation
from pyspark.sql import SparkSession
from delta import *

def create_delta_tables():
    # Initialize Spark with Delta Lake support
    spark = SparkSession.builder \
        .appName("SalesLake Delta Setup") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Create sales delta table
    spark.sql("""
    CREATE TABLE IF NOT EXISTS delta.`s3://saleslake-retail-data/bronze/sales/`
    (
        transaction_id STRING,
        store_id INT,
        product_id STRING,
        customer_id STRING,
        quantity INT,
        unit_price DECIMAL(10,2),
        total_amount DECIMAL(10,2),
        transaction_date TIMESTAMP,
        payment_method STRING
    )
    PARTITIONED BY (year INT, month INT, day INT)
    """)
    
    # Create customer delta table
    spark.sql("""
    CREATE TABLE IF NOT EXISTS delta.`s3://saleslake-retail-data/bronze/customer/`
    (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone STRING,
        address STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        registration_date TIMESTAMP,
        loyalty_tier STRING
    )
    """)
    
    # Create inventory delta table
    spark.sql("""
    CREATE TABLE IF NOT EXISTS delta.`s3://saleslake-retail-data/bronze/inventory/`
    (
        inventory_id STRING,
        product_id STRING,
        store_id INT,
        quantity INT,
        last_updated TIMESTAMP,
        min_stock_level INT,
        max_stock_level INT
    )
    PARTITIONED BY (year INT, month INT)
    """)
    
    spark.stop()

if __name__ == "__main__":
    create_delta_tables()