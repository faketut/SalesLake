# S3 bucket setup script
import boto3

def setup_s3_buckets():
    s3_client = boto3.client('s3')
    
    # Create main data lake bucket
    s3_client.create_bucket(
        Bucket='saleslake-retail-data',
        CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
    )
    
    # Create folders for different data domains
    domains = ['sales', 'customer', 'inventory', 'product']
    for domain in domains:
        for layer in ['bronze', 'silver', 'gold']:
            s3_client.put_object(
                Bucket='saleslake-retail-data',
                Key=f'{layer}/{domain}/'
            )
    
    # Set bucket policies for security
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Principal': {'AWS': 'arn:aws:iam::123456789012:role/SalesLakeETLRole'},
            'Action': ['s3:GetObject', 's3:PutObject'],
            'Resource': 'arn:aws:s3:::saleslake-retail-data/*'
        }]
    }
    
    s3_client.put_bucket_policy(
        Bucket='saleslake-retail-data',
        Policy=str(bucket_policy)
    )

if __name__ == "__main__":
    setup_s3_buckets()