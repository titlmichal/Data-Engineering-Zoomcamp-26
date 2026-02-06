CREATE OR REPLACE EXTERNAL TABLE `kestra-demo-485310.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://kestra-demo-485310-homework-bucket/yellow_tripdata_2024-*.parquet']
);

-- check whats in the external table
SELECT * FROM `kestra-demo-485310.zoomcamp.external_yellow_tripdata` LIMIT 10;

-- creation of non-external table
DROP TABLE IF EXISTS `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;
CREATE TABLE `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`
AS
SELECT * FROM `kestra-demo-485310.zoomcamp.external_yellow_tripdata`;

-- question 1
SELECT COUNT(*) FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;

-- question 2

SELECT COUNT(DISTINCT PULocationID)
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;

SELECT COUNT(DISTINCT PULocationID)
FROM `kestra-demo-485310.zoomcamp.external_yellow_tripdata`;

-- question 3
SELECT PULocationID
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;

SELECT PULocationID, DOLocationID
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;

-- question 4
SELECT COUNT(*)
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`
WHERE fare_amount = 0
;

-- question 5
CREATE TABLE `kestra-demo-485310.zoomcamp.yellow_tripdata_2024_optimized` 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;


-- question 6
SELECT DISTINCT VendorID
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15'
;

SELECT DISTINCT VendorID
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024_optimized`
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15'
;
