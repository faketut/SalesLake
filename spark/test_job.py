from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SalesLake Test") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read test data
sales_df = spark.read.parquet("s3a://saleslake-retail-data/bronze/sales/")
customer_df = spark.read.parquet("s3a://saleslake-retail-data/bronze/customers/")
inventory_df = spark.read.parquet("s3a://saleslake-retail-data/bronze/inventory/")

# Simple transformations
sales_summary = sales_df.groupBy("store_id").agg(
    {"total_amount": "sum", "transaction_id": "count"}
).withColumnRenamed("sum(total_amount)", "total_revenue") \
 .withColumnRenamed("count(transaction_id)", "transaction_count")

# Show results
print("Sales Summary by Store:")
sales_summary.show()

# Write back to S3 in Delta format
sales_summary.write.format("delta").mode("overwrite").save("s3a://saleslake-retail-data/silver/sales_summary/")

print("Test job completed successfully!")
