# Module 3: Data Warehousing 

- course page/md: https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse
- slides: https://docs.google.com/presentation/d/1a3ZoBAXFk8-EhUsd7rAZd-5p_HpltkzSeujjRGB2TAI/edit?slide=id.p#slide=id.p

## Data Warehouse

- YT video: https://www.youtube.com/watch?v=jrHljAoD6nM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=35
- OLAP x OLTP --> analytical x transactional
    - OLTP is for quick essential, real time, normalized DB for efficiency, generally small
    - OLAP is for big questions, periodical refreshed and scheduled, denormalized for analysis, generally big
    - OLTP is operations, productivity and end users
    - OLAP if for knowledge workers to make decisions
- DWH is OLAP solution --> used for reporting and analysis --> stores:
1) raw data
2) meta data
3) final data
- DWH can be transformed into data mart = smaller DWH for each given domain (e.g. sales, operations, ...)
- BigQuery is serverless DWH (no need to manage it) --> scale and availability
    - has built-in features like ML, geospatial analysis, BI
    - maximizes flexibility by separating compute X storage

### BigQuery Cost

- BQ offers on-demand and flate rate
- on-demand is simple: e.g. 1 TB of processed data is 5 USD
- flat rate is based on pre requested slots --> cca 100 slots = 2000 USD = 400 TB process data on demand
- plus in flat rate if I fill all slots, I need to wait until a query finishes so I can use the slot X on-deman will give new one
- idk, the video was shot in 2022 ... not sure how up to date this is

### Partition in BQ

- e.g. I often query data by some date column
- hence it would make sense to partition the data by this date column
- --> partition in this case would mean having e.g. one partition per one day in this date column
- --> then when BQ understands I have partitioned the table and query part of it --> it will not get the not needed partitions --> lower cost

### Clustering in BQ

- its like a add-on on top of the partitioning (my understanding)
- so now when the data is partitioned e.g. by date column, it can be clustered by other columns
- --> that means that within each partition, the data will be orderd by this cluster column to form own groups
- --> so it looks to me like level 2 partitioning

### BigQuery in practice

- BQ comes with some public data already there
- tables are reference like ```project-name``` + ```.``` + ```dataset-name``` + ```.``` + ```table-name```
- => e.g. ```bigquery-public-data.new_york_citibike.citibike_stations```
```SQL
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;
```
- btw when searching ```citibike_stations``` in the BQ resource I can find it
- --> then I can see quick details of each table: schema, details (size, rows, ...), preview ....
- BQ allows to create table from external sources --> ```external tables```
- this example is from a bucket (using data ingested to a bucket in module 2)
```SQL
CREATE OR REPLACE EXTERNAL TABLE `kestra-demo-485310.zoomcamp.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-demo-485310-homework-bucket/green_tripdata_2019-*.csv', 'gs://kestra-demo-485310-homework-bucket/green_tripdata_2020-*.csv']
);
```
- the ```*``` right before ```.csv``` and after the ```_2019-``` or ```_2020-``` tells it to use every file with name of this pattern (ending ```.csv``` and starting with the long "gs-bucket-file location in bucket" strin)
- BUT when checking this new table (```kestra-demo-485310.zoomcamp.external_green_tripdata```) in dataset --> NO info about size or records --> its bcs its not in BQ but externally saved (this time in GCS)
- how to partition in BQ?
```SQL
CREATE OR REPLACE TABLE `kestra-demo-485310.zoomcamp.green_tripdata_partitioned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `kestra-demo-485310.zoomcamp.external_green_tripdata`;
```
- the creation table syntax is generally the same:
    - ```CREATE OR REPLACE TABLE``` + ```table-name```
    - BUT then goes the partition by setup: ```PARTITION BY``` + ```column-name```
    - and ends as standard like ```AS SELECT * FROM ...;```
- PLUS NOW BQ knows the size and recors (bcs its in the BQ now...) + that its actually partitioned and how
- PLUS partition table logo is like double table, non-partitioned is simple one table
- so whats the impact then:
```SQL
-- bq approximates cca 104 MB to be queried
SELECT DISTINCT(VendorID)
FROM `kestra-demo-485310.zoomcamp.green_tripdata_non_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- bq approximates cca 608 KB to be queried
SELECT DISTINCT(VendorID)
FROM`kestra-demo-485310.zoomcamp.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';
```
- we can actually look into those partitions
- ... bcs its stored in internal tables (referenced like ```dataset-name``` + ```INFORMATION_SCHEMA.PARTITIONS```)
```SQL
SELECT table_name, partition_id, total_rows
FROM `zoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitioned'
ORDER BY total_rows DESC;
```
- its good for like minning how the partitions are working and stuff and there isnt e.g. a bias towards some partitions
- then there is clustering --> sort of additional partitioning = data within partitions are order by this cluster column
- how to cluster in BQ:
```SQL
CREATE OR REPLACE TABLE `kestra-demo-485310.zoomcamp.green_tripdata_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `kestra-demo-485310.zoomcamp.external_green_tripdata`;
```
- syntax is the same as when adding partitions BUT this ```CLUSTER BY``` + ```column-name``` is add between ```PARTITION``` and the actual ```SELECT ...```
- the selection of clustering column depends on the columns I want to filter on
- also in the info details of the table, the cluster info is shown

## Partioning and Clustering in detail

- YT video: https://www.youtube.com/watch?v=-CqXf7vhhDs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=25