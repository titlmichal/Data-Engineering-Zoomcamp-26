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
- 