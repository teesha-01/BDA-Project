import happybase
import pandas as pd

# HBase connection
connection = happybase.Connection('localhost', port=9090)
table = connection.table('fraud_age_analytics_hbase_table')

# Read the generated CSV data
df = pd.read_csv('C:/Users/NAWAB AHMAD/ecommerce-big-data-pipeline/data-generator/ecommerce_data.csv')

# Function to insert data into HBase in batch
def insert_batch_into_hbase(batch):
    with table.batch() as batch_writer:
        for _, row in batch.iterrows():
            row_key = str(row['transaction_id']).encode()
            data_dict = {
                b'fraud_data:user_id': str(row['user_id']).encode(),
                b'fraud_data:product_id': str(row['product_id']).encode(),
                b'fraud_data:category': str(row['category']).encode(),
                b'fraud_data:sub_category': str(row['sub_category']).encode(),
                b'fraud_data:price': str(row['price']).encode(),
                b'fraud_data:quantity': str(row['quantity']).encode(),
                b'fraud_data:total_amount': str(row['total_amount']).encode(),
                b'fraud_data:payment_method': str(row['payment_method']).encode(),
                b'fraud_data:timestamp': str(row['timestamp']).encode(),
                b'fraud_data:user_device': str(row['user_device']).encode(),
                b'fraud_data:user_city': str(row['user_city']).encode(),
                b'fraud_data:user_age': str(row['user_age']).encode(),
                b'fraud_data:user_gender': str(row['user_gender']).encode(),
                b'fraud_data:user_income': str(row['user_income']).encode(),
                b'fraud_data:fraud_label': str(row['fraud_label']).encode(),
            }
            batch_writer.put(row_key, data_dict)

# Insert data in batches for efficiency
batch_size = 1000  # Adjust this size according to your system's capacity
for start in range(0, len(df), batch_size):
    end = start + batch_size
    insert_batch_into_hbase(df.iloc[start:end])

# Close the connection
connection.close()
