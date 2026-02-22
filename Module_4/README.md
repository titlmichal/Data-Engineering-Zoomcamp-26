# Module 4: Analytics engineering

- module page: https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering
- goal: transforming DWH loaded data into analytical views using dbt project
- 2 options of approach:
1) cloud setup (BigQuery): gcp project with enabled BQ, service account and NYC taxi data loaded (both yellow and green for 2019-20)
2) local setup: nothing needed, the course will go through load into duckdb
- --> Im going with GCP

## Cloud setup

- cloud setup guide: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/setup/cloud_setup.md
- YT video: https://www.youtube.com/watch?v=GFbwlrt6f54
1) verify BQ setup
- service account with neccesary permissions (check - using the one I used previously - ```kestra-demo``` project - I have the data there)
- green and yellow data 2019/20 --> using Kestra like in module 2 and 3 to backfill (takes literally ages bcs its huge)
- remeber dataset loocation (US/EU/...) --> use same for dbt config (mine is ```europe-central2```)
2) sign up for dbt platform
- already have that
3) create new dbt project
- had one already there from previous times --> had to delete it bcs only 1 project at a time is free
- new project name: ```taxi_rides_ny```
4) configure BQ connection
- setting up the BQ connection (with service account, time out, maximum bites billed)
- dataset to be ```dbt_prod``` --> dbt will create a dataset as such and organize models into: ```dbt_prod_staging```, ```dbt_prod_intermediate```, ```dbt_prod_marts```
- then test connection
5) setup repo
- let dbt manage the repo OR connect own github repo
- --> going connect my own (need to install dbt app into the repo, after connecting the github and dbt accounts)
- (sort of painful bcs after each step - connection setup, github connection, dbt install in repo, ... - have to click through each again)
6) verify dev envi
- dev envi
    - personal workspace, personal credentials
    - creates temp schemas with my name (e.g. ```dbt_mt_...```)
    - changes only work, not production
    - used in dbt cloud IDE
- deployment envi
    - production workspace with models running on schedule
    - service account credentials
    - production schema creation (e.g. ```dbt_prod_staging```)
    - used by schedules to keep DWH updated
- basically like draft and published folders
- dev is created automatically
- to verify it is: deploy --> environments --> should be there
- idk but comparing to both the guide and the video, its a bit different what I see in my dbt platform (e.g. I didnt have the dev envi option when starting project), ... lets how it will work
7) start developing
- the guide has a sort of different way (going through option shown after dbt project creation), but going to the studio and initializing dbt project
- this course will be mostly about dbt cloud/plaform IDE but creating/setting up the CLI (from the platform) and VSCode extension can allow for development there

## Introduction to analytics engineering

- YT link: https://www.youtube.com/watch?v=HxMIsPrIyGQ
- AE = breaking the gap between DA and DE
- can be called many other ways as well (data modelling, data warehousing, BI engineer...) but its not that much what but how
    - now the emphasis is to make things not only big but robust
- AE is about analytical tasks (= data modelling) while keeping software engineering best practices
    - --> the shift is from what to how (robustly, reliably, safely, ...) --> with emphasis on robustness/safety
- still big part of AE is merging 2 or more different tables together (e.g. client tables from different source systems)
    - but AE is/can also be a lot about talking with stakeholders (incl. C-level) to e.g. discuss how to represent business in data (= data modelling)
    - ... and sometimes AE is about what DA and/or DE doesnt want to do
    - --> role of AE allows to dip into both DA and DE tasks

... finished at 31:24/1:23:48

## Analytics Engineering Basics

- YT link: https://www.youtube.com/watch?v=uF76d5EmdtU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=41
- DE prepares and maintains infrastructure, DA uses data to answer questions and solve problems
    - --> now DS and DA write more and more code due to recent development of dataflow but they are not primarly in that
    - --> DE are trained in coding but not in data usage etc
- --> AE comes in
    - taking care of modelling mainly but also: load, storage and presentation
- ETL X ELT
    - ETL can take longer but more stable and compliant but more expansive
    - ELT is faster and flexible and cheaper
- Kimbals dimensional modeling
    - goal: deliver understandable data to business users + query performance
    - apporach: prioritise understandability and performance over non redundant data (3NF)
    - --> less normalization (less joins needed)
- fact X dimensional tables
    - fact tables basically store measures, performance, ..., there are the verbs of the process
    - dimension are entities to described business --> provide context (e.g. customer, vendor, ...)
- architecture of dimensional modeling (kitchen metaphor)
    1) stage area
    - contains raw data exposed to only limited users
    2) processing area
    - raw to data models
    - focus on efficiency but still limited to knowladgeable users
    3) presentation area
    - exposure to wider business users

## What is dbt?

- YT link: https://www.youtube.com/watch?v=gsKuETFJr54&list=PLaNLNpjZpzwgneiI-Gl8df8GCsPYp_6Bs&index=6
- transformation workflow using SQL/Python to deploy analytics code following SW engineering best practices (modularity, portability, CI/CD, documentation)
- stands between raw and transformed data, sits on top of DWH
- --> introduces multiple layers:
    - develop layer
    - test and document layer
    - deployment (versioning, ci/cd, ...)
- how does it work?
    - each model is sql file
    - select statements, no DDL/DML
    - when doing ```dbt run```, dbt will compile and run everythin in DWH
- 2 ways of using dbt
1) dbt core
    - that is the stuff above (sql file, running, ...)
    - open-source dbt project (sql and yaml files)
    - has compilation logic, macros, DB adapters, CLI interface, free
2) dbt cloud
    - SaaS app running dbt core
    - IDE and cloud CLI
    - managed envis, job orchestration, logs and alerting, integrated docs, semantic layer
- --> that effects way of working with it: BQ --> dbt cloud X local DB --> dbt core
- BUT now there is also dbt fusion (!)
    - rewritten dbt core with faster compiling, new features, ...
    - but not supported by all DBs (e.g. duckDB) but works for main big ones (BQ, redshift, databricks, snowflake, ...)

## dbt Project Structure

- YT link: https://www.youtube.com/watch?v=2dYDS4OQbT0
- now I have the IDE ready with dbt connected to BQ and my zoomcamp repo
    - dbt creates its own branch
    - so then I ```commit``` (in the IDE) and ```create a pull request on GitHub``` (still in IDE)
    - and when in github I set up ```merge request``` and merge the branch with main
    - and then in my local machine i just do ```git checkout main``` and ```git pull origin main```
    - --> this is the SW engineering practices... in practice of AE
- when running dbt init (or respective action in dbt cloud) multiple files and dirs are created:
    1) ```analyses```: 
    - SQL script not to be shared but somehow useful for me, not purely test, more like admin/compliance/data quality reports
    2) ```dbt_project.yaml```
    - most important file in dbt (name of project/profile, defaults/variable setups, ...)
    - everytime some ```dbt``` command is run its checks for this file first
    - in dbt core, profiel should mathc the one in profiles directory
    3) ```macros```
    - like custom python functions, reusable/encapsulated logic, can be tested
    - good for saving e.g. definitions, conversions, tax rating ... --> when its changed one day, just change it in macro (not in every data model)
    4) ```README.md```
    - documentation of project ... simple readme
    - having info how to run, whom to contact, ... (I already had that)
    5) ```seeds```
    - feature of dbt for csv uploads
    - rather dirty/quick fix/one time/testing feature, but when I want a csv file as a table/data model in DWH, this is the way
    6) ```snapshots```
    - as name suggests, takes a snapshot of given table/data model and its values
    - next time the snapshot is run, extra row is added for the new value (X instead of overwritting it)
    - not best to be used in dbt (rather better in source), but its there
    7) ```tests```
    - many ways to tests in dbt, but this one is place for SQL file considered as assertions
    - e.g. we want timestamp per each hour --> checking that there is always 24hrs per given day of data
    - if query returns 0 rows => FAIL (```dbt build``` command fails)
    - aka singular tests
    8) ```models```
    - most important directory, here is the SQL (or even Python) logic
    - generally suggested 3 subdirectories: stagging --> intermediate --> marts
    - ```stagging``` = SQL sources, raw tables, 1:1 copy of data w/o cleaning
    - ```intermediate``` = not raw, not end user data ... (more complex joins, logics, ...)
    - ```marts``` = data for end users/ready for consumption, cleaned and modelled
    - ... these are dbt recommendations, but its up to me --> e.g. bronze/silver/gold

# dbt Sources

- YT link: https://www.youtube.com/watch?v=7CrrXazV_8k
- sources = tells dbt where the data is
- ```models``` directory --> creating ```stagging``` subdirectory
- creating there ```_sources.yaml``` file
```yaml
version: 1

sources:
  - name: raw_data
    description: "Raw data for NYC taxi"
    database: kestra-demo-485310
    schema: zoomcamp
    tables:
      - name: yellow_tripdata
      - name: green_tripdata
```
- the ```database```, ```schema``` and ```tables``` HAVE to match the GCP setup
- ```database``` is the GCP project id, ```schema``` is the dataset name
- ```raw_data``` is the name of this specific source
- now the actual stagging model --> creatin ```stg_green_tripdata.sql``` file there
```SQL
select * 
from {{ source("raw_data", "green_tripdata")}}
```
- its using the jinja code block (see the ```{{ }}```) and source function in there (1st arg is source name, 2nd is table)
- now I can ```preview``` or ```build``` (just running it fails, bcs there is too much data to be shown - bcs of the ```bytes billed``` limit I set before --> changing)
- it would make sense to do the cosmetic changes here: column selection, renaming/aliasing, type casting, ...
```SQL
 select
        -- identifiers
        cast(vendorid as integer) as vendor_id,
        {{ safe_cast('ratecodeid', 'integer') }} as rate_code_id,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(lpep_pickup_datetime as timestamp) as pickup_datetime,  -- lpep = Licensed Passenger Enhancement Program (green taxis)
        cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        ...
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        {{ safe_cast('payment_type', 'integer') }} as payment_type
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where vendorid is not null
```
- now it would make sense to use CTEs for better overview:
```SQL
with 
source as (
select 
    *
from {{ source("raw_data", "green_tripdata")}}
),

renamed as (
    select
        -- identifiers
        cast(vendorid as integer) as vendor_id,
        {{ safe_cast('ratecodeid', 'integer') }} as rate_code_id,
        cast(pulocationid as integer) as pickup_location_id,
        ...
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        {{ safe_cast('payment_type', 'integer') }} as payment_type
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where vendorid is not null
)

select * from renamed
```
- ...doing similar thing for ```stg_yellow_tripdata.sql``` (though its too big to preview)

## dbt Models

- YT link: https://www.youtube.com/watch?v=JQYz-8sl1aQ
- up until now I could do this by myself only (if it was real) --> now I need to not only explore but also discuss with business
- where next? --> start thinking what I want to build as a mart
- --> creating ```marts``` directory + ```reporting``` subdirectory + ```monthly_locations_revenue.sql``` file there
- --> I want proper dimensions/dimensional model with facts in the DWH --> ```dim_vendors.sql```, ```dim_locations.sql``` and ```fct_trips.sql```
- good modelling = simple aggreations to answer data questions
- starting with fact table (```fct_trips.sql```) --> need to union green and yellow data --> ```intermediate``` directory + ```int_trips_unioned.sql```
```SQL
with green_trips as (
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        ...
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),
yellow_trips as (
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        ...
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
)
select * from green_trips
union all
select * from yellow_trips
```
- it uses jinja again, this time for referencing models ```{{ ref('stg_green_tripdata') }}``` --> the ```ref()``` function is used for dbt models (```source``` is for raw sources)

## dbt Seeds and Macros

- YT link: https://www.youtube.com/watch?v=lT4fmTDEqVk
- seeds = dbt feature allowing for manual csv (or other) uploads
    - e.g. for testing or when not having ability to write to DWH directly
    - lives in ```seeds``` directory --> run ```dbt seed``` in dbt core/hit ```build``` in dbt cloud
    - --> now its available as a data model in dbt (e.g. via ```{{ ref('name_of_the_model')}}```)
    - (!) dont commit personal data + keep data small
    - see e.g. ```dim_zones.sql``` (previously as ```dim_locations.sql``)
- macros = custom functions, written in SQL files
    - see ```macros``` directory, e.g. ```get_vendor_data.sql``` or ```get_vendor_names.sql```
    - INTERESTING thing happened: when commeting out a macro, the compile still renders it --> only part of it is commented out --> fails
```SQL
{% macro get_vendor_names(vendor_id) -%}
case
    when {{ vendor_id }} = 1 then 'Creative tech'
    when {{ vendor_id }} = 2 then 'Verifone'
    when {{ vendor_id }} = 4 then 'Unknown'
end
{%- endmacro %}
```
- then I can easily reference it with jinja as other stuff ```{{ get_vendor_names("vendor_id") }} as vendor_name```
- --> will compile as:
```
case
    when vendor_id = 1 then 'Creative tech'
    when vendor_id = 2 then 'Verifone'
    when vendor_id = 4 then 'Unknown'
end
```

## dbt Tests

- YT link: https://www.youtube.com/watch?v=bvZ-rJm7uMU
- singular tests
    - simple (and sometimes complex) SQL queires
    - if it runs MORE than 0 rows => FAILED test
```SQL
select
    order_id,
    sum(amount) as total_amount
from {{ ref('fct_payments')}}
group by all
having sum(amount) < 0
```
- source freshness tests
    - set up in ```dbt_project.yaml``` file
    - then run a ```dbt source freshness``` command in CLI
    - e.g. (example from docs)
```yaml
sources:
  - name: jaffle_shop
    database: raw
    config: 
      freshness: # default freshness
        # changed to config in v1.9
        warn_after: {count: 12, period: hour}
        error_after: {count: 24, period: hour}
      loaded_at_field: _etl_loaded_at 

tables:
      - name: orders
        config:
          freshness: # make this a little more strict
            warn_after: {count: 6, period: hour}
            error_after: {count: 12, period: hour}
```
- generic tests
    - also defined in ```dbt_project.yaml``` file
    - 4 built-in subtypes: unique, not_null, accepted_values, relationships (each value should have counter-part in defined other side)
    - PLUS custom tables can be written as SQL/jinja (in ```tests/generic``` dir) and then referenced as built-in ones
    - PLUS dbt community has built many of others
    - each test is column-level = setting up tests per each column
```yaml
columns:
  - name: order_id
    tests:
      - unique
      - not_null
  - name: status
    tests:
      - accepted_values:
          values: ['pending', 'completed', 'cancelled']
  - name: customer_id
    tests:
      - relationships:
          to: ref('customers')
          field: id
```
```SQL
{% test test_positive_values(model, column_name) %}
{{ config(severity = 'warn')}}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} <= 0
{% endtest %}
```
- unit tests
    - not that new, but not often used as well
    - so this rather data model test, not data that much
    - so works like: if I give you this input data, this should be the output
```yaml
unit_tests:
  - name: test_is_valid_email_address
    description: "Check my is_valid_email_address logic captures all known edge cases - emails without ., emails without @, and emails from invalid domains."
    model: dim_customers
    given:
      - input: ref('stg_customers')
        rows:
          - {email: cool@example.com,    email_top_level_domain: example.com}
          - {email: cool@unknown.com,    email_top_level_domain: unknown.com}
          - {email: badgmail.com,        email_top_level_domain: gmail.com}
          - {email: missingdot@gmailcom, email_top_level_domain: gmail.com}
      - input: ref('top_level_email_domains')
        rows:
          - {tld: example.com}
          - {tld: gmail.com}
    expect:
      rows:
        - {email: cool@example.com,    is_valid_email_address: true}
        - {email: cool@unknown.com,    is_valid_email_address: false}
        - {email: badgmail.com,        is_valid_email_address: false}
        - {email: missingdot@gmailcom, is_valid_email_address: false}
```
- model contracts
    - so this tests whether the model (im trying to build) matches the structure underneath
    - so the contracts guarantee that the model will be as agreed in the contract
```yaml
models:
  - name: dim_customers
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: int
        constraints:
          - type: not_null
      - name: customer_name
        data_type: string
```
- ... there are others tests but mostly more further in the pipeline (e.g. CI/CD)

## dbt documentation

- YT link: https://www.youtube.com/watch?v=UqoWyMjcqrA
```yaml
sources:
  - name: raw_data
    description: "Raw data for NYC taxi"
    database: kestra-demo-485310
    schema: zoomcamp
    tables:
      - name: yellow_tripdata
        descrition: The bigger yellow dataset
        columns:
          - name: vendorid
            description: ID of given vendor executing the ride
            data_type: integer
            meta:
              pii: false
              ownership: data_team
              importance: high
```
- ```meta``` section is for internal uses usually --> can set anything there (pii = personal info, owner, ...)
- this is example is for ```_sources.yaml``` but almost anything in dbt can be documented with yaml (stagging models, marts, ...)
- --> for marts its usually named like ```schema.yaml```
- --> also its maybe sometimes good to have one yaml per one model (to avoid super long ones)
- AND the (sort of) IMPORTANT thing here: (some) ```tests``` are to be defined in those yamls (!)
```yaml
models:
  - name: dim_zones
    description: Taxi zone dimension table with location details
    columns:
      - name: location_id
        description: Unique identifier for each taxi zone
        data_tests:
          - unique
          - not_null
      - name: borough
        description: NYC borough name
      - name: zone
        description: Specific zone name within the borough
      - name: service_zone
        description: Service zone classification

  - name: dim_vendors
    description: Taxi technology vendor dimension table
    columns:
      - name: vendor_id
        description: Unique vendor identifier
        data_tests:
          - unique
          - not_null
      - name: vendor_name
        description: Company name of the vendor
```
- and there are also dbt commands useful here (in dbt core): ```dbt docs generate```
- --> generates a ```catalog.json``` file = representation of EVERYTHING in documentation (plus some other stuff in models)
- --> used to generate a website for technical documentation
- in cloud, ```dbt docs generate``` is enough (allegedly there is a tick box for it*) X for dbt core, extra is needed: ```dbt docs serve```
    - *its built in the cloud UI
- --> website is openned with documentation (by default at localhost:8080 --> need to figure out how to host elsewhere if needed)