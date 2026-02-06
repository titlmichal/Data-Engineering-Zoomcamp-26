# Module 3: homework

- assignment: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/03-data-warehouse/homework.md
- submission form: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw3
- in general:
1) get Jan2024 to Jun2024 yellow data to GCP bucket
2) create external table from those parquets
3) answer questions
- solution:
1) done with Kestra --> see ```gcp_yellow_taxi_2024.yaml```
2) done within BQ --> see ```gcp_yellow_taxi_2024.sql```
3) questions below

## Questions

### Question 1. Counting records

_What is count of records for the 2024 Yellow Taxi Data?_

--> 20 332 093

### Question 2. Data read estimation

_Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables._
_What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?_

```SQL
SELECT COUNT(DISTINCT PULocationID)
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;

SELECT COUNT(DISTINCT PULocationID)
FROM `kestra-demo-485310.zoomcamp.external_yellow_tripdata`;
```

--> 0 MB for the External Table and 155.12 MB for the Materialized Table

### Question 3. Understanding columnar storage

_Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table._
_Why are the estimated number of Bytes different?_

```SQL
SELECT PULocationID
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;

SELECT PULocationID, DOLocationID
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;
```

--> BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

### Question 4. Counting zero fare trips

_How many records have a fare_amount of 0?_

--> 8 333

### Question 5. Partitioning and clustering

_What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)_

--> Partition by tpep_dropoff_datetime and Cluster on VendorID

```SQL
CREATE TABLE `kestra-demo-485310.zoomcamp.yellow_tripdata_2024_optimized` 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`;
```

### Question 6. Partition benefits

_Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)_

_Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?_

_Choose the answer which most closely matches._

--> 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

```SQL
SELECT DISTINCT VendorID
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024`
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15';

SELECT DISTINCT VendorID
FROM `kestra-demo-485310.zoomcamp.yellow_tripdata_2024_optimized`
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15';
```

### Question 7. External table storage

_Where is the data stored in the External Table you created?_

--> GCP Bucket

### Question 8. Clustering best practices

_It is best practice in Big Query to always cluster your data:_

--> False