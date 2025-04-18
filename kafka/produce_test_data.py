from kafka import KafkaProducer
import json
import pandas as pd
import time
from datetime import datetime

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load test data
sales_df = pd.read_parquet('test_data/sales/sales_data.parquet')

# Convert to records
sales_records = sales_df.to_dict('records')

# Send records to Kafka
for record in sales_records:
    # Convert any datetime objects to strings
    for k, v in record.items():
        if isinstance(v, datetime):
            record[k] = v.isoformat()
    
    # Send record
    producer.send('sales-topic', record)
    print(f"Sent record: {record['transaction_id']}")
    time.sleep(0.2)  # Small delay

producer.flush()
print("All records sent!")
