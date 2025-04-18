import pandas as pd
import numpy as np
import datetime as dt
import uuid
import os

# Create directories
os.makedirs('test_data/sales', exist_ok=True)
os.makedirs('test_data/customers', exist_ok=True)
os.makedirs('test_data/inventory', exist_ok=True)

# Generate sales data
def create_sales_data(num_records=1000):
    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2023, 1, 31)
    
    data = {
        'transaction_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'store_id': np.random.randint(1, 11, num_records),
        'product_id': [f'P{np.random.randint(100, 1000)}' for _ in range(num_records)],
        'customer_id': [f'C{np.random.randint(1000, 10000)}' for _ in range(num_records)],
        'quantity': np.random.randint(1, 10, num_records),
        'unit_price': np.random.uniform(5.0, 100.0, num_records).round(2),
        'transaction_date': [start_date + dt.timedelta(
            days=np.random.randint(0, 31),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        ) for _ in range(num_records)],
        'payment_method': np.random.choice(['Credit', 'Debit', 'Cash', 'Mobile'], num_records)
    }
    
    df = pd.DataFrame(data)
    df['total_amount'] = (df['quantity'] * df['unit_price']).round(2)
    return df

# Generate customer data
def create_customer_data(num_records=500):
    data = {
        'customer_id': [f'C{i+1000}' for i in range(num_records)],
        'first_name': [f'FirstName{i}' for i in range(num_records)],
        'last_name': [f'LastName{i}' for i in range(num_records)],
        'email': [f'customer{i}@example.com' for i in range(num_records)],
        'phone': [f'555-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}' for _ in range(num_records)],
        'address': [f'{np.random.randint(100, 9999)} Main St' for _ in range(num_records)],
        'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], num_records),
        'state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'], num_records),
        'zip_code': [f'{np.random.randint(10000, 99999)}' for _ in range(num_records)],
        'registration_date': [dt.datetime(2022, 1, 1) + dt.timedelta(days=np.random.randint(0, 365)) for _ in range(num_records)],
        'loyalty_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], num_records)
    }
    return pd.DataFrame(data)

# Generate inventory data
def create_inventory_data(num_records=200):
    data = {
        'inventory_id': [f'INV{i+1000}' for i in range(num_records)],
        'product_id': [f'P{np.random.randint(100, 1000)}' for _ in range(num_records)],
        'store_id': np.random.randint(1, 11, num_records),
        'quantity': np.random.randint(0, 1000, num_records),
        'last_updated': [dt.datetime(2023, 1, 1) + dt.timedelta(days=np.random.randint(0, 31)) for _ in range(num_records)],
        'min_stock_level': np.random.randint(10, 50, num_records),
        'max_stock_level': np.random.randint(100, 500, num_records)
    }
    return pd.DataFrame(data)

# Generate and save data
sales_df = create_sales_data()
sales_df.to_parquet('test_data/sales/sales_data.parquet', index=False)

customer_df = create_customer_data()
customer_df.to_parquet('test_data/customers/customer_data.parquet', index=False)

inventory_df = create_inventory_data()
inventory_df.to_parquet('test_data/inventory/inventory_data.parquet', index=False)

print("Test data generated successfully!")
