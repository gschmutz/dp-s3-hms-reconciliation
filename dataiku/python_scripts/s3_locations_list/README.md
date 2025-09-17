# Create S3 location list with buckets

This sub-project creates a list of s3 locations for tables registered in Hive Metastore. The data is partitioned into variable amount of buckets. The output is written in CSV format to a file and can optionally be uploaded to S3.

## Run the comparision

Set environment variables

```bash
export FILTER_DATABASE=
export FILTER_TABLE=

export BATCHING_STRATEGY=balanced_by_partition_size
export NUMBER_OF_BUCKETS=3

export HMS_DB_ACCESS_STRATEGY=trino

export HMS_DB_USER=hive
export HMS_DB_PASSWORD=abc123!
export HMS_DB_HOST=localhost
export HMS_DB_PORT=5442
export HMS_DB_DBNAME=metastore_db 

export HMS_TRINO_USER=trino
export HMS_TRINO_PASSWORD=
export HMS_TRINO_HOST=localhost
export HMS_TRINO_PORT=28082
export HMS_TRINO_CATALOG=hive_metastore_db
export HMS_TRINO_USE_SSL=false

export S3_UPLOAD_ENABLED=false
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://localhost:9000
export S3_ADMIN_BUCKET=admin-bucket
export S3_LOCATION_LIST_OBJECT_NAME=s3_locations.csv
```

Run it

```bash
python create_location_list.py
```
