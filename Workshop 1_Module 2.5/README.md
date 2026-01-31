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

... stopped at 27:34/1:30:54