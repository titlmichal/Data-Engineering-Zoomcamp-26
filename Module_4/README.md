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
    - next time the snapshot is run, extra row is added for the new value
    - not best to be used in dbt (rather better in source), but its there
    7) ```tests```
    8) ```models```

... stopped at 11:39/20:22