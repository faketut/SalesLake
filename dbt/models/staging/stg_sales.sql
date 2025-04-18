-- Sales staging model
-- models/staging/stg_sales.sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'transaction_id'
  )
}}

SELECT
    transaction_id,
    store_id,
    product_id,
    customer_id,
    quantity,
    unit_price,
    total_amount,
    transaction_date,
    payment_method,
    _etl_loaded_at
FROM
    {{ source('snowflake_stage', 'SALES_STAGING') }}
{% if is_incremental() %}
    WHERE _etl_loaded_at > (SELECT MAX(_etl_loaded_at) FROM {{ this }})
{% endif %}

-- models/staging/stg_customers.sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_id'
  )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    zip_code,
    registration_date,
    loyalty_tier,
    _etl_loaded_at
FROM
    {{ source('snowflake_stage', 'CUSTOMER_STAGING') }}
{% if is_incremental() %}
    WHERE _etl_loaded_at > (SELECT MAX(_etl_loaded_at) FROM {{ this }})
{% endif %}

-- models/staging/stg_inventory.sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'inventory_id'
  )
}}

SELECT
    inventory_id,
    product_id,
    store_id,
    quantity,
    last_updated,
    min_stock_level,
    max_stock_level,
    _etl_loaded_at
FROM
    {{ source('snowflake_stage', 'INVENTORY_STAGING') }}
{% if is_incremental() %}
    WHERE _etl_loaded_at > (SELECT MAX(_etl_loaded_at) FROM {{ this }})
{% endif %}

-- models/intermediate/int_customer_profile.sql
{{
  config(
    materialized = 'table'
  )
}}

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.loyalty_tier,
    c.registration_date,
    COUNT(DISTINCT s.transaction_id) as total_transactions,
    SUM(s.total_amount) as lifetime_spend,
    MAX(s.transaction_date) as last_purchase_date,
    DATEDIFF('day', MAX(s.transaction_date), CURRENT_DATE()) as days_since_last_purchase
FROM
    {{ ref('stg_customers') }} c
LEFT JOIN
    {{ ref('stg_sales') }} s
    ON c.customer_id = s.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.loyalty_tier,
    c.registration_date

-- models/marts/fact_sales.sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'transaction_id'
  )
}}

SELECT
    s.transaction_id,
    s.store_id,
    s.product_id,
    s.customer_id,
    s.quantity,
    s.unit_price,
    s.total_amount,
    s.transaction_date,
    s.payment_method,
    p.product_name,
    p.product_category,
    p.product_subcategory,
    p.unit_cost,
    (s.total_amount - (s.quantity * p.unit_cost)) as gross_profit,
    c.loyalty_tier,
    st.store_name,
    st.region,
    st.store_format
FROM
    {{ ref('stg_sales') }} s
JOIN
    {{ ref('stg_products') }} p
    ON s.product_id = p.product_id
LEFT JOIN
    {{ ref('stg_customers') }} c
    ON s.customer_id = c.customer_id
JOIN
    {{ ref('stg_stores') }} st
    ON s.store_id = st.store_id
{% if is_incremental() %}
    WHERE s.transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
{% endif %}

-- models/marts/dim_store_inventory.sql
{{
  config(
    materialized = 'table'
  )
}}

SELECT
    i.inventory_id,
    i.product_id,
    i.store_id,
    i.quantity as current_quantity,
    i.min_stock_level,
    i.max_stock_level,
    p.product_name,
    p.product_category,
    p.unit_cost,
    p.unit_price,
    st.store_name,
    st.region,
    CASE
        WHEN i.quantity <= i.min_stock_level THEN 'Restock Required'
        WHEN i.quantity < (i.min_stock_level * 1.5) THEN 'Low Stock'
        WHEN i.quantity > (i.max_stock_level * 0.9) THEN 'Overstocked'
        ELSE 'Healthy'
    END as inventory_status,
    i.last_updated
FROM
    {{ ref('stg_inventory') }} i
JOIN
    {{ ref('stg_products') }} p
    ON i.product_id = p.product_id
JOIN
    {{ ref('stg_stores') }} st
    ON i.store_id = st.store_id