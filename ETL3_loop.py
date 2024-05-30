import time
import pyodbc
import pandas as pd
import boto3
from io import StringIO

# Database connection parameters
server = 'localhost'  # Replace with SQL Server name or IP address
database = 'AdventureWorksDW2022'  # Database name
driver = '{ODBC Driver 17 for SQL Server}'  # The correct ODBC driver installed

# AWS S3 parameters
s3_bucket_name = 'rytualetlprojectbucket'  # S3 bucket name
aws_region = 'us-east-2'  # AWS region

# List of tables to query
tables = ['DimAccount', 'DimDepartmentGroup', 'DimEmployee', 'DimGeography', 'DimOrganization', 'DimProduct']
file_prefix = 'currency_data'
interval = 3 * 60  # Interval in seconds
total_duration = 12 * 60  # Total duration in seconds
num_iterations = total_duration // interval  # Number of iterations

# Initialize the iteration count
iteration = 3  # Start counting from 3 

# Function to fetch data and upload to S3
def fetch_and_upload_data(table_name, iteration):
    # SQL query to extract data
    sql_query = f"SELECT * FROM {table_name};"
    
    # Connect to SQL Server and fetch data
    print(f"Connecting to SQL Server to fetch data from {table_name}...")
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;')
    print("Executing query...")
    data = pd.read_sql(sql_query, conn)
    print("Data fetched successfully.")
    conn.close()
    
    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    
    # Generate the S3 key
    s3_key = f"data/{table_name}_{file_prefix}{iteration}.csv"
    
    # Upload to S3
    print(f"Uploading data to S3 as {s3_key}...")
    s3_client = boto3.client('s3', region_name=aws_region)
    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print("Data has been uploaded to S3 successfully.")

# Loop to execute the task periodically
start_time = time.time()
for i in range(num_iterations):
    for table in tables:
        fetch_and_upload_data(table, iteration)
        iteration += 1
    # Sleep for the interval duration
    time.sleep(interval - ((time.time() - start_time) % interval))

print("ETL job completed.")