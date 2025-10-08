import boto3
import time
import pandas as pd

# === CONFIGURATION ===
endpoint_url = 'http://your-onprem-s3-url:9000'
access_key = 'your-access-key'
secret_key = 'your-secret-key'
bucket_name = 'your-bucket-name'

# Files to wait for
file_keys = ['incoming/file1.csv', 'incoming/file2.csv']
local_files = ['/tmp/file1.csv', '/tmp/file2.csv']

# Polling interval (in seconds)
poll_interval = 10

# === CONNECT TO S3 ===
s3 = boto3.client(
    's3',
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name='us-east-1',  # dummy region for on-prem
)

# === CHECK IF FILE EXISTS ===
def file_exists(key):
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise

# === WAIT FOR BOTH FILES ===
def wait_for_files():
    print(f"Waiting for files:\n- {file_keys[0]}\n- {file_keys[1]}")
    found = {key: False for key in file_keys}

    while not all(found.values()):
        for key in file_keys:
            if not found[key] and file_exists(key):
                print(f"[✔] Found: {key}")
                found[key] = True
        if not all(found.values()):
            time.sleep(poll_interval)

# === DOWNLOAD FILES ===
def download_files():
    for key, local_path in zip(file_keys, local_files):
        print(f"Downloading {key} → {local_path}")
        s3.download_file(bucket_name, key, local_path)

# === COMPARE FILES USING PANDAS ===
def compare_files():
    try:
        df1 = pd.read_csv(local_files[0])
        df2 = pd.read_csv(local_files[1])

        # Optional: sort to make comparison order-independent
        df1_sorted = df1.sort_values(by=df1.columns.tolist()).reset_index(drop=True)
        df2_sorted = df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True)

        # Compare: rows that are different (in either file)
        diff = pd.concat([df1_sorted, df2_sorted]).drop_duplicates(keep=False)

        print("\n=== Differences Between CSV Files ===")
        if diff.empty:
            print("✅ No differences found. Files are identical (row-wise).")
        else:
            print(diff)
    except Exception as e:
        print(f"⚠️ Error comparing files: {e}")

# === MAIN ===
if __name__ == "__main__":
    wait_for_files()
    download_files()
    compare_files()
    print("\n✅ Done. Exiting.")
