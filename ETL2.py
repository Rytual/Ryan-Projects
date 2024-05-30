import pyodbc
import pandas as pd
import boto3
from io import StringIO

# Database connection parameters
server = 'localhost'  # Replace with SQL Server name or IP address
database = 'AdventureWorksDW2022'  # Database name
driver = '{ODBC Driver 17 for SQL Server}'  # Ensure the correct ODBC driver installed

# AWS S3 parameters
s3_bucket_name = 'rytualetlprojectbucket'  # S3 bucket name
s3_key = 'data/currency_data2.csv'  # Desired S3 key (file path)

# AWS Region
aws_region = 'us-east-2'  # AWS region

# SQL query to extract data
sql_query = """
select AccountKey,
AccountCodeAlternateKey,
ParentAccountKey
from DimAccount
join DimReseller
on NumberEmployees = dimaccount.AccountKey
where AccountKey > 10
and ParentAccountCodeAlternateKey > 5000
and ParentAccountKey != 88
order by AccountType asc;;
"""

# Connect to SQL Server and fetch data
print("Connecting to SQL Server...")
conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;')
print("Executing query...")
data = pd.read_sql(sql_query, conn)
print("Data fetched successfully.")
conn.close()

# Convert DataFrame to CSV
csv_buffer = StringIO()
data.to_csv(csv_buffer, index=False)

# Upload to S3
print("Uploading data to S3...")
s3_client = boto3.client('s3', region_name=aws_region)
s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
print("Data has been uploaded to S3 successfully.")