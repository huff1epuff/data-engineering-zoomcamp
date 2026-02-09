# SQL queries used for homework 3

## Creating tables
```
CREATE OR REPLACE EXTERNAL TABLE `python-for-drive.zoomcamp.yellow_tripdata`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://python_for_drive_dezoomcamp_hw3_2026/yellow_tripdata_2024-*.parquet']
);
```

```
CREATE OR REPLACE TABLE `python-for-drive.zoomcamp.yellow_tripdata_unpartitioned` 
AS SELECT * FROM `zoomcamp.yellow_tripdata`;
```

### Question 1
```
select 
    count(*) as record_count
from
    `zoomcamp.yellow_tripdata`;
```
> answer is `20332093`

### Question 2
```
select 
    count(distinct(PULocationID)) as unique_PULocationID_count 
from
    `zoomcamp.yellow_tripdata`; 
```
> estimated `0 B`

```
select 
    count(distinct(PULocationID)) as unique_PULocationID_count 
from 
    `zoomcamp.yellow_tripdata_unpartitioned`;
```
> estimated `155.12 MB`

### Question 4
```
select 
    count(*) 
from 
    `zoomcamp.yellow_tripdata_unpartitioned`
where 
    fare_amount = 0;
```
> answer is `8333`

### Question 5
```
CREATE OR REPLACE TABLE `python-for-drive.zoomcamp.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
    SELECT * FROM `zoomcamp.yellow_tripdata`
);
```

### Question 6
```
select 
    distinct(VendorID)
from
    `zoomcamp.yellow_tripdata_partitioned_clustered`
where
    tpep_dropoff_datetime >= '2024-03-01' 
    and tpep_dropoff_datetime < '2024-03-16';
```
> answer is `310.24 MB`

```
select 
    distinct(VendorID)
from
    `zoomcamp.yellow_tripdata_partitioned_clustered`
where
    tpep_dropoff_datetime >= '2024-03-01' 
    and tpep_dropoff_datetime < '2024-03-16';
```
> answer is `26.84 MB`
