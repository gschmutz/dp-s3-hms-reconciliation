# Compare HMS database against baseline

This sub-project contains the logic to repair the Hive Metastore. It works for both Hive Metastore 3.x and 4.x. 


## Create the baseline

Set environment variables

```bash
export DATAIKU_ENV=pz

export FILTER_TABLES=
export FILTER_TIMESTAMP=

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

export S3_UPLOAD_ENABLED=true
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=abc123abc123
export S3_ENDPOINT_URL=http://localhost:9000
export S3_ADMIN_BUCKET=admin-bucket
export HMS_BASELINE_OBJECT_NAME={zone}_baseline_hms.csv
```

Run create baseline

```bash
python create_baseline_from_hms.py
```

## Run the comparision

```bash
export DATAIKU_ENV=pz
export FILTER_DATABASE=flight_db
#export FILTER_TABLES=
export FILTER_BATCH=
export FILTER_STAGE=

export HMS_DB_ACCESS_STRATEGY=trino
export USE_BASELINE_TIMESTAMP=false

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
export S3_BASELINE_OBJECT_NAME={zone}_baseline_hms.csv
```

Run `pytest`

```bash
pytest test_hms_compare.py --verbose
```


The comparison is driven by the s3 locations in the file.