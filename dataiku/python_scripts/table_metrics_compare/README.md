# Database comparision using `pytest`

This sub-project contains a `pytest` to compare the partitions in S3 (based on a file with the baseline) against the data in Hive Metastore Database. For each partitioned table, it does a compare of the 

 * the count of partitions 
 * fingerprint (hash value) for the concatination of the sorted list of partition names

A draft version of the script for creating the baseline is available as well.

## Prepare environment

```bash
python3.11 -m venv myenv
source venv/bin/activate

pip install -r requirements.txt
```

## Create the baseline from S3

```bash
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://localhost:9000
export S3_BUCKET_NAME=flight-bucket
export S3_PREFIX=refined

export S3_BASELINE_BUCKET=admin-bucket
export S3_BASELINE_OBJECT_NAME=baseline_s3.csv
```

Run the process for creating the baseline (calculating the number of partitions and the fingerprint of all the partition names

```bash
python create-baseline-from-s3.py
```

An example of the exported file is shown below

```csv
s3_location,partition_count,timestamp,fingerprint
s3a://flight-bucket/refined/flights_0_t,24,1753211845,ed3a4fc1db3ad3ddf87012aaaaffc9fad61672bc6f9cf11365d466fa8f50d03d
s3a://flight-bucket/refined/flights_1_t,24,1753211845,ed3a4fc1db3ad3ddf87012aaaaffc9fad61672bc6f9cf11365d466fa8f50d03d
s3a://flight-bucket/refined/flights_2_t,24,1753211846,ed3a4fc1db3ad3ddf87012aaaaffc9fad61672bc6f9cf11365d466fa8f50d03d
s3a://flight-bucket/refined/flights_3_t,24,1753211846,ed3a4fc1db3ad3ddf87012aaaaffc9fad61672bc6f9cf11365d466fa8f50d03d
s3a://flight-bucket/refined/flights_4_t,24,1753211846,ed3a4fc1db3ad3ddf87012aaaaffc9fad61672bc6f9cf11365d466fa8f50d03d
s3a://flight-bucket/refined/flights_5_t,24,1753211846,ed3a4fc1db3ad3ddf87012aaaaffc9fad61672bc6f9cf11365d466fa8f50d03d
s3a://flight-bucket/refined/flights_6_t,24,1753211846,ed3a4fc1db3ad3ddf87012aaaaffc9fad61672bc6f9cf11365d466fa8f50d03d
```

## Run the comparision

Set environment variables

```bash
export METASTORE_DB_ACCESS_STRATEGY=postgresql
export HMS_DB_HOST=localhost
export HMS_DB_PORT=5442
export HMS_DB_USER=hive
export HMS_DB_PASSWORD=abc123!
export HMS_DB_DBNAME=metastore_db

export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://localhost:9000
export S3_BASELINE_BUCKET=admin-bucket
export S3_BASELINE_OBJECT_NAME=baseline_s3.csv
```

```json
    "HMS_DB_HOST": "localhost",
    "HMS_DB_PORT": 5442,
    "HMS_DB_DBNAME": "metastore_db",

    "S3_ENDPOINT_URL": "http://minio-1:9000",
    "S3_BASELINE_BUCKET": "admin-bucket",
    "S3_BASELINE_OBJECT_NAME": "baseline_s3.csv"
```

```bash
    HMS_DB_USER=hive
    HMS_DB_PASSWORD=abc123!
```

Run `pytest`

```bash
pytest compare-partitions.py --verbose
```

The comparision is driven by the s3 locations in the file.