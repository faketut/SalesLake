-- External stage setup
-- Create external stage to S3
CREATE OR REPLACE STAGE RETAIL_S3_STAGE_STREAMING
  URL = 's3://saleslake-retail-data/bronze/sales_streaming/'
  CREDENTIALS = (AWS_KEY_ID = 'your_access_key_id' AWS_SECRET_KEY = 'your_secret_access_key')
  FILE_FORMAT = (TYPE = PARQUET);

-- Create external table pointing to streaming data
CREATE OR REPLACE EXTERNAL TABLE RETAIL_STAGE.SALES_STREAMING_EXT
  LOCATION = @RETAIL_S3_STAGE_STREAMING
  AUTO_REFRESH = TRUE
  FILE_FORMAT = (TYPE = PARQUET)
  PATTERN = '.*[.]parquet';

-- Create materialized view with near real-time refresh
CREATE OR REPLACE MATERIALIZED VIEW RETAIL_DW.MARTS.REALTIME_SALES_DASHBOARD
AUTO_REFRESH = TRUE
AS
SELECT 
    DATE_TRUNC('HOUR', s.transaction_date) as sale_hour,
    s.store_id,
    st.store_name,
    st.region,
    p.product_category,
    COUNT(DISTINCT s.transaction_id) as transactions,
    SUM(s.quantity) as units_sold,
    SUM(s.total_amount) as revenue,
    AVG(s.total_amount) as avg_transaction_value
FROM 
    RETAIL_STAGE.SALES_STREAMING_EXT s
JOIN 
    RETAIL_DW.DIMENSIONS.DIM_STORES st ON s.store_id = st.store_id
JOIN 
    RETAIL_DW.DIMENSIONS.DIM_PRODUCTS p ON s.product_id = p.product_id
WHERE
    s.transaction_date >= DATEADD(DAY, -1, CURRENT_TIMESTAMP())
GROUP BY 
    DATE_TRUNC('HOUR', s.transaction_date),
    s.store_id,
    st.store_name,
    st.region,
    p.product_category;

-- Create SQL procedure for refreshing views
CREATE OR REPLACE PROCEDURE RETAIL_UTILS.REFRESH_REALTIME_VIEWS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Refresh external table
    ALTER EXTERNAL TABLE RETAIL_STAGE.SALES_STREAMING_EXT REFRESH;
    
    -- Refresh materialized view
    ALTER MATERIALIZED VIEW RETAIL_DW.MARTS.REALTIME_SALES_DASHBOARD REFRESH;
    
    RETURN 'Successfully refreshed real-time views at ' || CURRENT_TIMESTAMP();
END;
$$;

-- Create task to automatically refresh every 5 minutes
CREATE OR REPLACE TASK RETAIL_UTILS.REFRESH_REALTIME_VIEWS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
CALL RETAIL_UTILS.REFRESH_REALTIME_VIEWS();

-- Start the task
ALTER TASK RETAIL_UTILS.REFRESH_REALTIME_VIEWS_TASK RESUME;