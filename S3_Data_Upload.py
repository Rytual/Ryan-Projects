import boto3

# AWS S3 parameters
s3_bucket_name = 'mybucket'
file_path = 'data.csv'
s3_key = 'data/data.csv'

# Upload to S3
s3_client = boto3.client('s3')
s3_client.upload_file(file_path, s3_bucket_name, s3_key)
print("File uploaded successfully.")
