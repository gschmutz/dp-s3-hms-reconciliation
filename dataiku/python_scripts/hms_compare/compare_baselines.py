import boto3
import time
import json



aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY

# Track seen files
seen_files = set()

# Config for on-prem S3 (e.g. MinIO)
s3 = boto3.client(
    's3',
    endpoint_url='http://your-onprem-s3-url:9000',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key',
    region_name='us-east-1',  # arbitrary
)

bucket_name = 'your-bucket-name'
poll_interval = 10  # seconds

def list_objects():
    response = s3.list_objects_v2(Bucket=bucket_name)
    return [obj['Key'] for obj in response.get('Contents', [])]

while True:
    print("Checking for new files...")
    try:
        current_files = set(list_objects())

        # Detect new files
        new_files = current_files - seen_files
        for key in new_files:
            print(f"New file detected: {key}")
            # You can now download and process the file
            local_path = f'/tmp/{key.split("/")[-1]}'
            s3.download_file(bucket_name, key, local_path)

            # TODO: Compare the new file here
            # Compare the files (line by line, as an example)
            with open(file1_local, 'r') as f1, open(file2_local, 'r') as f2:
            f1_lines = f1.readlines()
            f2_lines = f2.readlines()

            # Simple diff
            diff_lines = [line for line in f1_lines if line not in f2_lines]

            print("Lines in file1 but not in file2:")
            for line in diff_lines:
              print(line.strip())

        seen_files = current_files

    except Exception as e:
        print(f"Error while polling: {e}")

    time.sleep(poll_interval)
