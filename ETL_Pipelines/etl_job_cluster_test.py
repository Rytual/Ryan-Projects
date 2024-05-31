from pyspark.sql import SparkSession
import boto3
from io import StringIO

# AWS S3 parameters
s3_bucket_name = 'rytualetlprojectbucket'  # Your S3 bucket name
s3_key = 'scripts/etl_job_cluster_test.py'  # Desired S3 key (file path)

# Spark session
spark = SparkSession.builder \
    .appName("ETLJob") \
    .getOrCreate()

# SQL query to extract data
sql_query = """
SELECT CurrencyName 
FROM dbo.DimCurrency 
WHERE CurrencyKey < 5;
"""

# Connect to SQL Server and fetch data
print("Connecting to SQL Server...")
# Code to connect to SQL Server and fetch data using PyODBC goes here

# Mocking data retrieval with pandas DataFrame for demonstration
import pandas as pd
data = pd.DataFrame({'CurrencyName': ['Afghani', 'Algerian Dinar', 'Argentine Peso', 'Armenian Dram']})

# Convert DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(data)

# Write Spark DataFrame to S3 as CSV
print("Writing data to S3...")
csv_buffer = StringIO()
spark_df.write.csv(f"s3://{s3_bucket_name}/{s3_key}", mode='overwrite', header=True)

print("Data has been written to S3 successfully.")