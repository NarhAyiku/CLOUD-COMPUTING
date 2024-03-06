import boto3
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# AWS S3 credentials
aws_access_key_id = 'YOUR_AWS_ACCESS_KEY_ID'
aws_secret_access_key = 'YOUR_AWS_SECRET_ACCESS_KEY'
aws_bucket_name = 'YOUR_AWS_BUCKET_NAME'

# Azure Blob Storage credentials
azure_connection_string = 'YOUR_AZURE_CONNECTION_STRING'
azure_container_name = 'YOUR_AZURE_CONTAINER_NAME'

# Initialize AWS S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Initialize Azure Blob Storage client
blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

# Get list of objects in AWS S3 bucket
s3_objects = s3_client.list_objects(Bucket=aws_bucket_name)

# Transfer each object from AWS S3 to Azure Blob Storage
for s3_object in s3_objects['Contents']:
    object_key = s3_object['Key']
    # Download object from AWS S3
    response = s3_client.get_object(Bucket=aws_bucket_name, Key=object_key)
    data = response['Body'].read()
    # Upload object to Azure Blob Storage
    blob_client = blob_service_client.get_blob_client(container=azure_container_name, blob=object_key)
    blob_client.upload_blob(data)
    print(f"Transferred object {object_key} from AWS S3 to Azure Blob Storage.")
