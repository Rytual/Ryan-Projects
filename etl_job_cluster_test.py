from pyspark.sql import SparkSession
import boto3
from io import StringIO

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Job") \
    .getOrCreate()

# Database connection parameters
server = '192.168.1.156'  # Your SQL Server IP address
database = 'AdventureWorksDW2022'  # Your database name
username = 'your_sql_username'  # Your SQL Server username
password = 'your_sql_password'  # Your SQL Server password
jdbc_url = f"jdbc:sqlserver://{server};databaseName={database}"

# AWS S3 parameters
s3_bucket_name = 'rytualetlprojectbucket'  # Your S3 bucket name
s3_key = 'data/currency_data.csv'  # Desired S3 key (file path)
aws_region = 'us-east-2'  # Your AWS region

# SQL query to extract data
sql_query = """
SELECT CurrencyName 
FROM dbo.DimCurrency 
WHERE CurrencyKey < 5;
"""

# Connect to SQL Server and fetch data
print("Connecting to SQL Server...")
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"({sql_query}) as subquery") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

print("Data fetched successfully.")

# Convert DataFrame to CSV and upload to S3
csv_buffer = StringIO()
df.toPandas().to_csv(csv_buffer, index=False)

print("Uploading data to S3...")
s3_client = boto3.client('s3', region_name=aws_region)
s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
print("Data has been uploaded to S3 successfully.")

# Stop Spark session
spark.stop()