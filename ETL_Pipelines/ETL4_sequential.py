import pyodbc
import pandas as pd
import boto3
from io import StringIO
import time

# Database connection parameters
server = 'localhost'  # Replace with the SQL Server name or IP address
database = 'AdventureWorksDW2022'  # Database name
driver = '{ODBC Driver 17 for SQL Server}'  # Ensure the correct ODBC driver is installed

# AWS S3 parameters
s3_bucket_name = 'rytualetlprojectbucket'  # S3 bucket name

# AWS Region
aws_region = 'us-east-2'  # AWS region

# List of tables to extract data from
tables = ['DimAccount', 'DimDepartmentGroup', 'DimEmployee', 'DimGeography', 'DimOrganization', 'DimProduct']

# Keep track of file numbers for each table
file_numbers = {table: 1 for table in tables}

# Function to perform the ETL process for a given table
def etl_process(table, file_number):
    # SQL query to extract data
    sql_query = f"SELECT * FROM {table};"

    # Connect to SQL Server and fetch data
    print(f"Connecting to SQL Server to fetch data from {table}...")
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;')
    print(f"Executing query for {table}...")
    data = pd.read_sql(sql_query, conn)
    print(f"Data fetched successfully from {table}.")
    conn.close()

    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    # Define S3 key with sequential numbering
    s3_key = f"data/{table}_currency_data{file_number}.csv"

    # Upload to S3
    print(f"Uploading data to S3 for {table} as {s3_key}...")
    s3_client = boto3.client('s3', region_name=aws_region)
    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Data has been uploaded to S3 successfully for {table}.")

# Loop to run the ETL process for each table every 3 minutes for 12 minutes
start_time = time.time()
end_time = start_time + 12 * 60  # Run for 12 minutes

while time.time() < end_time:
    for table in tables:
        etl_process(table, file_numbers[table])
        file_numbers[table] += 1
    time.sleep(3 * 60)  # Wait for 3 minutes before the next run

print("ETL process completed.")