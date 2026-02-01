# Workshop 1_Module 2.5: 

- API reading and pipeline scalability
- data normalization and incremental loading
- data lake with dlt

(btw again creatin virtual envi not to mess up and keep best practices)

... according to the main course page, the workshop about data ingestion should go between moduke 2 and 3 (https://github.com/DataTalksClub/data-engineering-zoomcamp?tab=readme-ov-file)
- the workshop page: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/workshops/dlt/README.md
- the workshop colab page: https://colab.research.google.com/drive/1FiAHNFenM8RyptyTPtDTfqPCi5W6KX_V?usp=sharing
- 3 parts of the workshop
1) data extraction (with scale)
2) data normalization (clean and structure before load)
3) load and incremental updates
- next steps (e.g. project inspiration) in the workshop page

- now stuff below is mainly from these related sources:
1) workshop file: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/workshops/dlt/data_ingestion_workshop.md
2) the workshop video: https://www.youtube.com/watch?v=pgJWP_xqO1g

# Data ingestion with dlt

- building robust pipelines, following best practices and e.g. loading into datalake
- notebook will be enough, BUT only at same point load to bq --> credentials needed there (i guess will be okay, used them before)

### What is ingestion?

- process of extraction data from sources --> transporting and preparing it (cleaning, normalizing, adding metadata)
- parquet, avro, db tables, ... = well-structured with explicit schema --> can be used immediately
- csv, json, ... = unstructured/not well defined --> cleaning and formatting 
- schem includes format, structure, types, relationships, ...
- pipelines are here to to extract, transform and load ... basically sort this out
- usual steps? (*DE is mostly about first 3-4)
1) collect: data store, streams, apps, ...
2) ingest: bulk (e.g. DB) X continuous (e.g. json from app, event queue)
3) store: data lake (files stored as (parquet) files, cheap storage but long compute) X data warehouse (e.g. bq, ms sql) X data lakehouse (mix of the two: files stored + additional layer of metadata --> easier searching)
4) compute: batch X stream
5) consume: science, BI, self-service, ML

- what does the DE do here:
1) optimize data storage
2) ensure data quality and integrity (duplicates, NAs, ...)
3) implement governance (compliance, well-managed)
4) adapt data architectures (to account for changing needs of organization)
--> entire ```data lifecycle``` management
--> this workshop: robust pipeline (collect + ingest + store) --> ETL

- btw dbt is for transformation (SQL, consume part of DE) X dlt is for ingestions (collect and ingest part of DE)

## Extracting data

- 99 % of data in DE is from DBs, APIs or files --> workshop is about APIs --> least structured but most common
- 3 common types of APIs
1) RESTful: usually from business apps (e.g. CRMs) or even e.g. Spotify has RESTful API, Pokemon, movie DB ...
2) file-based: usually returns bulk data like JSON or Parquet
3) database APIs: e.g. MongoDB or SQL, usually return JSON

- what to keep in mind when running pipelines:
1) HW limits: memory (RAM) and storage, overload --> whole system might crash (e.g. gcp has 12.7 GB RAM)
2) network reliability: always account for retries (e.g. dlt has features for this)
3) API rate limits: too many requests --> fail or even block (check docs for limits)
4) other...: pagination, authentication, ...

## Working with REST APIs

- REST = representational state transfer (API), each is sort of unique
- via HTTP requests (GET, POST, PUT, DELETE) but has challenges
- e.g. github api:
```python
import requests
result = requests.get("https://api.github.com/orgs/dlt-hub/repos").json()
print(result[:2])
```

- this is public API, so there is no e.g. authentication, but other APIs might want that or other stuff
- --> 4 challenges:
1) rate limits
- standard API has some limit of requests
- what to do?
- check and monitor API rate limits (e.g. some APIs provide the counter of remaining requests in header ... or check docs)
- pause requests when near the limit
- some provide retry-after header --> how long to wait
- implement auto retries
2) authentication
- some require that
- 3 types: API keys (simple token in header or url), OAuth tokens (more secure), basic authentication (e.g. like username and paswword, less common)
- do NOT share tokens publicly --> in code e.g. like envi variable (or e.g. colab has keys feature)
3) pagination
- APIs dont return all data once --> chunks or pages of results
- to get all --> multiple iterative requests
- see example in ```examples.ipynb``` (or sample below of it)
```python
page_nr = 1
while True:
    params = {"page": page_nr}
    response = requests.get(URL, params=params)
    page_data = response.json()

    # empty page = no data anymore
    if not page_data:
        break

    print(page_data)
    page_nr += 1
```
- each API is designed carefully --> pagination can be different --> adjust accordingly
4) memory issues during extraction
- limited memory in pipelines, e.g. serveless (e.g. lambda) functions or shared clusters
- --> solution: batch processing/streaming data
- choosing the batch size to avoid memory overflowing
- e.g. APIs to files, webhooks to event queues, queues to buckets
- see example in ```examples.ipynb``` (or sample below of it)
```python
def paginater():
    page_nr = 1
    while True:
        params = {"page": page_nr}
        try:
            response = requests.get(URL, params=params)
            response.raise_for_status() # if 200-299 => nothing, if 4xx or 5xx => error risen + details
            response = response.json()
            print(f"Got page {page_nr} with {len(response)} records.")

            if response:
                yield response  # WHATS THIS AND WHY?
                page_nr += 1
            else:
                break
        except Exception as e:
            print(e)
            break

for page_data in paginater():
    print(page_data)
    break
```
- this ```yield response``` changes the function into ```generator``` --> when ```yield``` is reached in the function, its "paused" and data is returned
- --> if it was simple ```return``` I would have to save all pages into one one object --> with this, I can iterate one by one to e.g. load into DWH
- --> at one point: only one page in memory
- --> easy memory management X low throughput (e.g. bcs rate limits or response time)
- --> dlt can simply this while keeping good perfomance

## Extracting data with dlt

- custom scripts deal with pagination, rate limits, auth, errros, ... --> dlt simplies this with a built-in REST API client
- --> built in REST API support, auto pagination handling, rate limits and retries management, streaming support, seamless integration
- dlt is open-source working in 3 steps: extract, normalize, load
- helps with moving data from many sources (incl REST APIs) to many destination
- just need to do ```pip install dlt``` (or ```uv add dlt``` or even ```uv add dlt[duckdb]``` to get dlt with duckdb as destination)
- btw duckDB is lightweight DB that can be spin up in local machine memory
```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient # this CLIENT helps with the challenges with standard approaches (RAM, rates, ...)
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

def paginated_getter():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net", # same as above
        # Define pagination strategy - page-based pagination (but there are others)
        paginator=PageNumberPaginator(   # <--- Pages are numbered (1, 2, 3, ...)
            base_page=1,   # <--- Start from page 1
            total_path=None    # <--- No total count of pages provided by API, pagination should stop when a page contains no result items
        )
    )

    # the paginate function uses the endpoint (it already knows base url from above)
    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   # remember about memory management and yield data (again, will give one interation and then pause)


for page_data in paginated_getter():
    print(page_data)
    break
```
- --> no iteration for pagination was needed
- --> if there was a limit error --> dlt would wait and try when possible
- (dlt can read ```retry_after``` from headers, sometimes even ```pagination counters``` or similar)
- btw Kafka can be used as simple ingestion tool too but more likely to be a source
- btw I can use dlt to extract to even local files (csv, parquet, ...)
- ...generally its quite universal (e.g. sometimes GCP bigquery can be source, sometimes destination) --> check ```dlt docs``` for main sources (rest, dbs and clouds) + they have ```verified-sources```

## Normalizing data

- 1) normalizing = putting a schema and structurue to them w/o change of meaning (about this is mostly the workshop)
- 2) filtering for given use case = usually for given analysis
- ... can be called ```data cleaning``` in general
- --> big part of it: ```metadata work``` (giving data structure and standard) = adding types, renaming columns, flattenning nested dicts, unnesting into child tables, ... (bcs of naming conventions in DWH, DBs tables relationships, ...)
- why to do this and not use JSON directly? its good for data transfer BUT NO: enforced schema, consistent data types, hard to process at once, heavy for memory, slow for analysis (X e.g. parquet) --> GOOD for exchange X BAD for direct analysis
- the NYC data from the API is already normalized (by the workshop creators) but initially it wasnt:
- e.g. before:
```
"coordinates": {
    "start": {"lat": 40.641525, "lon": -73.787442},
    "end": {"lat": 40.742963, "lon": -73.980072}
}
```
- and now:
```
{'End_Lat': 40.742963,
 'End_Lon': -73.980072,
    ...
 'Start_Lat': 40.641525,
 'Start_Lon': -73.787442,
    ...}
```
- AND dlt does quite a lot of things automatically:
1) detects schema (data types)
2) flattens nested JSON (complex into table-ready format)
3) handles data type conversions (e.g. dates, nrs, bools, ...)
4) splits lists into child tables (ensures relational integrity)
5) adapts to schema changes over time (will be covered LATER)
- HOW to do it MYSELF:
```python
import dlt

# 1) DEFINE PIPELINE FOR AUTO NORMALIZATION
pipeline = dlt.pipeline(
    pipeline_name="taxi_example",
    destination="duckdb",
    dataset_name="taxi"
)
print(pipeline)

# 2) RUN THE PIPELINE WITH RAW NESTED DATA
info = pipeline.run(data=data, table_name="rides", write_disposition="replace")
    # --> .duckdb file is created/changed

# 3) PRINT THE RESULT SUMMARY
print(info)
```
- usually the data is some kind of resource (in this example its just basic dict with some nesting)
- check below for more what happened behind:
```python
print(pipeline.last_trace)
```
- metadata of the extraction: how long it took, what has it done etc.
- now check the final dataset:
```python
import pandas
pipeline.dataset().rides.df()
```
- dataset() method + which table + make it into df
- or check the columns ```pipeline.dataset().rides.df().columns```
- it also included some columns starting with ```_``` --> internal system columns with ids
- PLUS the second table that was created ```pipeline.dataset().rides__passengers.df()```
- BTW if using multiple sources, check docs and map function (BUT dlt doesnt recommend to load multiple sources into one table)
- BTW the data types are first transformed to internal ones and then to the ones according to the target destination (see docs)
- BTW it can show sort of UI via ```dlt.pipeline.show()``` (in .py file) --> spins of ```streamlit``` app with the pipeline, tables, etc.

## Loading data

- w/o dlt:
1) schema validation
2) batch processing
3) error handling
4) retries etc.
5) plus the stanard: connection, table creation and schemas, writting queries, ...
```python
# LOADING DATA WITHOUT DLT
import duckdb

# 1) CREATING CONNECTION TO IN-MEMORY DUCKDB
conn = duckdb.connect("taxi_manual.db")

# 2) CREATE A TABLE
conn.execute("""
CREATE TABLE IF NOT EXISTS rides (
    record_hash TEXT PRIMARY KEY,
    vendor_name TEXT,
    pickup_time TIMESTAMP,
    dropoff_time TIMESTAMP,
    start_lon DOUBLE,
    start_lat DOUBLE,
    end_lon DOUBLE,
    end_lat DOUBLE
);
"""
)

# 3) INSERT DATA MANUALLY
data = [
    {
        "vendor_name": "VTS",
        "record_hash": "b00361a396177a9cb410ff61f20015ad",
        "time": {
            "pickup": "2009-06-14 23:23:00",
            "dropoff": "2009-06-14 23:48:00"
        },
        "coordinates": {
            "start": {"lon": -73.787442, "lat": 40.641525},
            "end": {"lon": -73.980072, "lat": 40.742963}
        }
    }
]
# FLATTEN FOR INSERTION
flattened_data = [
    (
        ride["record_hash"],
        ride["vendor_name"],
        ride["time"]["pickup"],
        ride["time"]["dropoff"],
        ride["coordinates"]["start"]["lon"],
        ride["coordinates"]["start"]["lat"],
        ride["coordinates"]["end"]["lon"],
        ride["coordinates"]["end"]["lat"]
    )
    for ride in data
]
#ACTUAL INSERT
conn.executemany("""
INSERT INTO rides (record_hash, vendor_name, pickup_time, dropoff_time, start_lon, start_lat, end_lon, end_lat)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", flattened_data)

print("Load was a success!")

# 4) QUERY THE DATA
conn.execute("SELECT * FROM rides").df()

# 5) CLOSE THE CONNECTION
conn.close()
```
- --> schema management is manual
- --> no auto retries if it fails
- --> no incremental loading
- --> more code to maintain


- with dlt?
- as mentioned above mostly:
1) multiple destinations
2) performance optmized (batch, parallelism, streaming)
3) schema-aware (checks it matches destinations requirements)
4) incremental load (--> only new/failed/updated to be inserted)
5) resilience and retries (--> loads data w/o missing records)
- --> if load FINISHED => data was loaded FULLY --> can be trusted
- how to do it myself?
```python
...
```




... stopped at 1:01:16/1:30:54