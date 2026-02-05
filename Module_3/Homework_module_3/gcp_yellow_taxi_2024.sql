CREATE OR REPLACE EXTERNAL TABLE `kestra-demo-485310.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://kestra-demo-485310-homework-bucket/yellow_tripdata_2024-*.parquet']
);

-- check whats in the external table
SELECT * FROM `kestra-demo-485310.zoomcamp.external_yellow_tripdata` LIMIT 10;