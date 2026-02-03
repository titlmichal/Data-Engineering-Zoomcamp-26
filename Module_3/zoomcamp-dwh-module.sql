-- how to create external table
CREATE OR REPLACE EXTERNAL TABLE `kestra-demo-485310.zoomcamp.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-demo-485310-homework-bucket/green_tripdata_2019-*.csv', 'gs://kestra-demo-485310-homework-bucket/green_tripdata_2020-*.csv']
);

-- check whats in the external table
SELECT * FROM `kestra-demo-485310.zoomcamp.external_green_tripdata` LIMIT 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `kestra-demo-485310.zoomcamp.green_tripdata_non_partitioned` AS
SELECT * FROM `kestra-demo-485310.zoomcamp.external_green_tripdata`;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `kestra-demo-485310.zoomcamp.green_tripdata_partitioned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `kestra-demo-485310.zoomcamp.external_green_tripdata`;

-- Impact of partition
-- bq approximates cca 104 MB to be queried
SELECT DISTINCT(VendorID)
FROM `kestra-demo-485310.zoomcamp.green_tripdata_non_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- bq approximates cca 608 KB to be queried
SELECT DISTINCT(VendorID)
FROM `kestra-demo-485310.zoomcamp.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- checking out partitions
SELECT table_name, partition_id, total_rows
FROM `zoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `kestra-demo-485310.zoomcamp.green_tripdata_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `kestra-demo-485310.zoomcamp.external_green_tripdata`;

-- bq approximates cca 61 MB to be queried
-- and actually 62 MB was processed
SELECT count(*) as trips
FROM `kestra-demo-485310.zoomcamp.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- bq approximates cca 61 MB to be queried
-- ... so the same actually 
-- ... and it actually processed 61 MB
-- --> probably this is due caching that BQ has set ON by default
SELECT count(*) as trips
FROM `kestra-demo-485310.zoomcamp.green_tripdata_partitioned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;