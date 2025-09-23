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

## Run the comparision

Set environment variables

```bash
export FILTER_CATALOG=
export FILTER_SCHEMA=
export FILTER_TABLES=
export FILTER_BATCH=

export TRINO_USER=trino
export TRINO_PASSWORD=
export TRINO_HOST=localhost
export TRINO_PORT=28082
export TRINO_CATALOG=hive
export TRINO_USE_SSL=false


export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://localhost:9000
export S3_ADMIN_BUCKET=admin-bucket
export S3_BASELINE_OBJECT_NAME=baseline_s3.csv
```

Run `pytest`

```bash
pytest test_table_metrics_compare.py --verbose
```

