# SQL queries used for homework 3

```
CREATE OR REPLACE EXTERNAL TABLE `python-for-drive.zoomcamp.yellow_tripdata`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://python_for_drive_dezoomcamp_hw3_2026/yellow_tripdata_2024-*.parquet']
);
```

```
select 
    count(*) as record_count
from
    `zoomcamp.yellow_tripdata`;
```
> answer is `20332093`

```
CREATE OR REPLACE TABLE `python-for-drive.zoomcamp.yellow_tripdata_unpartitioned` 
AS SELECT * FROM `zoomcamp.yellow_tripdata`;
```

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

```
select 
    count(*) 
from 
    `zoomcamp.yellow_tripdata_unpartitioned`
where 
    fare_amount = 0;
```
> answer is `8333`

```
CREATE OR REPLACE TABLE `python-for-drive.zoomcamp.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
    SELECT * FROM `zoomcamp.yellow_tripdata`
);
```