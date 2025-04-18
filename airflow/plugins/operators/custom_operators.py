# Custom operators for retail workflows
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'saleslake_retail_etl',
    default_args=default_args,
    description='ETL pipeline for retail sales data',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['retail', 'sales', 'etl'],
)

# Extract data from source systems to S3
extract_sales_data = SqlToS3Operator(
    task_id='extract_sales_data',
    query="SELECT * FROM retail_db.sales WHERE DATE(transaction_date) = '{{ ds }}'",
    s3_bucket='saleslake-retail-data',
    s3_key='bronze/sales/year={{ execution_date.year }}/month={{ execution_date.month }}/day={{ execution_date.day }}/sales_{{ ds }}.parquet',
    replace=True,
    sql_conn_id='retail_mysql_conn',
    aws_conn_id='aws_default',
    file_format='parquet',
    dag=dag,
)

extract_customer_data = SqlToS3Operator(
    task_id='extract_customer_data',
    query="SELECT * FROM retail_db.customers WHERE DATE(last_updated) = '{{ ds }}'",
    s3_bucket='saleslake-retail-data',
    s3_key='bronze/customer/customers_{{ ds }}.parquet',
    replace=True,
    sql_conn_id='retail_mysql_conn',
    aws_conn_id='aws_default',
    file_format='parquet',
    dag=dag,
)

extract_inventory_data = SqlToS3Operator(
    task_id='extract_inventory_data',
    query="SELECT * FROM retail_db.inventory WHERE DATE(last_updated) = '{{ ds }}'",
    s3_bucket='saleslake-retail-data',
    s3_key='bronze/inventory/year={{ execution_date.year }}/month={{ execution_date.month }}/inventory_{{ ds }}.parquet',
    replace=True,
    sql_conn_id='retail_mysql_conn',
    aws_conn_id='aws_default',
    file_format='parquet',
    dag=dag,
)

# Load data from S3 to Snowflake staging
load_sales_to_snowflake = S3ToSnowflakeOperator(
    task_id='load_sales_to_snowflake',
    s3_keys=['bronze/sales/year={{ execution_date.year }}/month={{ execution_date.month }}/day={{ execution_date.day }}/sales_{{ ds }}.parquet'],
    table='SALES_STAGING',
    schema='RETAIL_STAGE',
    stage='RETAIL_S3_STAGE',
    file_format='PARQUET',
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

load_customer_to_snowflake = S3ToSnowflakeOperator(
    task_id='load_customer_to_snowflake',
    s3_keys=['bronze/customer/customers_{{ ds }}.parquet'],
    table='CUSTOMER_STAGING',
    schema='RETAIL_STAGE',
    stage='RETAIL_S3_STAGE',
    file_format='PARQUET',
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

load_inventory_to_snowflake = S3ToSnowflakeOperator(
    task_id='load_inventory_to_snowflake',
    s3_keys=['bronze/inventory/year={{ execution_date.year }}/month={{ execution_date.month }}/inventory_{{ ds }}.parquet'],
    table='INVENTORY_STAGING',
    schema='RETAIL_STAGE',
    stage='RETAIL_S3_STAGE',
    file_format='PARQUET',
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

# Run dbt transformations on Snowflake data
run_dbt_transformations = SnowflakeOperator(
    task_id='run_dbt_transformations',
    sql="CALL RETAIL_UTILS.RUN_DBT_TRANSFORMS('{{ ds }}')",
    snowflake_conn_id='snowflake_conn',
    warehouse='TRANSFORM_WH',
    role='TRANSFORM_ROLE',
    database='RETAIL_DW',
    dag=dag,
)

# Create aggregated data marts
create_sales_mart = SnowflakeOperator(
    task_id='create_sales_mart',
    sql="""
    MERGE INTO RETAIL_DW.MARTS.DAILY_SALES_SUMMARY tgt
    USING (
        SELECT 
            store_id,
            DATE(transaction_date) as sale_date,
            SUM(total_amount) as daily_revenue,
            COUNT(DISTINCT transaction_id) as transaction_count,
            COUNT(DISTINCT customer_id) as customer_count,
            AVG(total_amount) as avg_basket_size
        FROM RETAIL_DW.TRANSFORMED.FACT_SALES
        WHERE DATE(transaction_date) = '{{ ds }}'
        GROUP BY store_id, DATE(transaction_date)
    ) src
    ON tgt.store_id = src.store_id AND tgt.sale_date = src.sale_date
    WHEN MATCHED THEN
        UPDATE SET
            tgt.daily_revenue = src.daily_revenue,
            tgt.transaction_count = src.transaction_count,
            tgt.customer_count = src.customer_count,
            tgt.avg_basket_size = src.avg_basket_size,
            tgt.last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (store_id, sale_date, daily_revenue, transaction_count, customer_count, avg_basket_size, last_updated)
        VALUES (src.store_id, src.sale_date, src.daily_revenue, src.transaction_count, src.customer_count, src.avg_basket_size, CURRENT_TIMESTAMP());
    """,
    snowflake_conn_id='snowflake_conn',
    warehouse='TRANSFORM_WH',
    role='TRANSFORM_ROLE',
    database='RETAIL_DW',
    dag=dag,
)

# Set task dependencies
extract_sales_data >> load_sales_to_snowflake
extract_customer_data >> load_customer_to_snowflake
extract_inventory_data >> load_inventory_to_snowflake

[load_sales_to_snowflake, load_customer_to_snowflake, load_inventory_to_snowflake] >> run_dbt_transformations
run_dbt_transformations >> create_sales_mart