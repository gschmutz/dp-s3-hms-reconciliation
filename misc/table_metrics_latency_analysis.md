# Analysis of table metrics latency

Create the CSV file with the 

## Create Trino table to wrap the csv file

```sql
CREATE  TABLE minio.flight_db.table_metrics_latency (
    table_name               VARCHAR,
    latency            VARCHAR,
    timestamp             VARCHAR
)
WITH (
    external_location = 's3a://admin-bucket/table_metrics_latency/',
    format = 'CSV',
    skip_header_line_count = 1
);
````

## Create a view with correct datatypes

```sql
CREATE OR REPLACE VIEW minio.flight_db.metrics_latency_v
AS
SELECT table_name
, CAST(latency as bigint) as latency_ms
, CAST(timestamp as bigint) as timestamp_ms
FROM minio.flight_db.table_metrics_latency;
```

## Analysis

After Monday, September 29, 2025 9:00:00 AM

```sql
select table_name
, min(latency_ms)/1000
, max(latency_ms)/1000
from minio.flight_db.metrics_latency_v
where timestamp_ms > 1759136400000
group by table_name;
```

Before Saturday, September 27, 2025 9:00:00 AM

```sql
select table_name
, min(latency_ms)/1000  / 60
, max(latency_ms)/1000 / 60
from minio.flight_db.metrics_latency_v
where timestamp_ms < 1758963600000
group by table_name
order by max(latency_ms) desc;
```