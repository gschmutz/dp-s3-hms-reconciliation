# Compare S3 to HMS

This sub-project contains the logic to repair the Hive Metastore. It works for both Hive Metastore 3.x and 4.x. 


## Run the comparision

Set environment variables

```bash
export FILTER_DATABASE=
export FILTER_TABLE=

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

export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://localhost:9000
export S3_ADMIN_BUCKET=admin-bucket
export S3_BASELINE_OBJECT_NAME=baseline_s3.csv
```

Run create baseline

```bash
python create_baseline_from_s3.py
```

Run `pytest`

```bash
pytest test_s3_hms_repair.py --verbose
```

The comparision is driven by the s3 locations in the file.