# Module 1 Homework: Docker & SQL

https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/01-docker-terraform/homework.md

Link for submissions: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw1

## Question 1: Understanding Docker images

Question:
_Run docker with the python:3.13 image. Use an entrypoint bash to interact with the container.
What's the version of pip in the image?_

Answer:

run ```docker run -it --entrypoint=bash python:3.13``` and then ```pip --version``` --> ```pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)```

--> ```25.3```

## Question 2: 

Question:
_Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?_
```
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

Answer: 
- hostname should be ```db``` (name of the service) or ```postgres``` (bcs containers in one network are referecing each other by names) 
- and port ```5432``` (first port = 5433 = local host port X second port = 5432 = container port + they are in one common network as they are run together via docker compose --> port 5433 is for local host inputs X internal port 5432 is for containers between each other)
- --> ```postgres:5432``` or ```db:5432```

## Data prep for questions 3-6

_Download data from wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet 
and https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv_

- --> I will recreate what I have done in the workshop for this purpose (and practice)
- start with init the project (and selecting respective interpreter, see notes for details)
```uv init --python=3.13```

- adding needed dependencies (the pyarrow in this version bcs the one one somehow fails)
```uv add pandas click tqdm sqlalchemy```
```uv add "pyarrow==22.0.0" ```
```uv add pgcli```
```uv add psycopg2-binary``` (sqlalchemy needs that)

- running ```uv sync``` to get all up to speed (if needed)

- running the docker container
```
docker run -it --rm 
  -e POSTGRES_USER="root" 
  -e POSTGRES_PASSWORD="root" 
  -e POSTGRES_DB="ny_taxi" 
  -v ny_taxi:/var/lib/postgresql 
  -p 5432:5432 
  postgres:18
```
- and pgcli to access the postgre db in container
```uv run pgcli -h localhost -p 5432 -u root -d ny_taxi```

- now I have to get the data there --> see the ```main.py``` file --> ```uv run main.py```
- ...I could also create docker file for the script, or even docker-compose yaml and set up network and use pgadmin
- ... but I kinda need to answer the questions now --> local run via docker volume and pgcli (see above)

## Question 3: Counting short trips

_For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?_

```SQL
SELECT 
    COUNT(*) 
FROM green_taxi_nov_25 
WHERE 
    CAST(lpep_pickup_datetime AS date) >= CAST('2025-11-01' AS date) 
    AND CAST(lpep_pickup_datetime AS date) <  CAST('2025-12-01'  AS date) 
    AND trip_distance <= 1;
```

--> 8007 trips

## Question 4: Longest trip for each day

_Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

Use the pick up time for your calculations._

```SQL
SELECT 
* 
FROM green_taxi_nov_25 
WHERE trip_distance < 100 
order by trip_distance 
DESC limit 1;
```
--> 2025-11-14 15:36:27

## Question 5: Biggest pickup zone

_Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?_

```SQL
SELECT
    *
FROM (
SELECT
    "green_taxi_nov_25"."PULocationID",
    SUM("green_taxi_nov_25"."total_amount")
FROM green_taxi_nov_25
WHERE CAST(lpep_pickup_datetime AS date) = CAST('2025-11-18' AS date)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1) AS "T1"
INNER JOIN taxi_zone_lookup "T2" ON
    "T1"."PULocationID" = "T2"."LocationID"
```
--> East Harlem North

## Question 6: Largest tip

_For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?_

Note: it's tip , not trip. We need the name of the zone, not the ID._

```SQL
SELECT
    *
FROM green_taxi_nov_25 AS "T1"
INNER JOIN taxi_zone_lookup "T2" ON
    "T1"."PULocationID" = "T2"."LocationID"
    AND "T2"."Zone" = 'East Harlem North'
INNER JOIN taxi_zone_lookup "T3" ON
    "T1"."DOLocationID" = "T3"."LocationID"
ORDER BY "T1"."tip_amount" DESC
LIMIT 1
```
--> Yorkville West

## Question 7: Terraform Workflow

- had create dedicated gcp project, service account and go through the policy disabling through GCP CLI to get keys
- then init --> plan --> apply

_Which of the following sequences, respectively, describes the workflow for:_

_Downloading the provider plugins and setting up backend,_
_Generating proposed changes and auto-executing the plan_
_Remove all resources managed by terraform`_

_Answers:_
_terraform import, terraform apply -y, terraform destroy_
_teraform init, terraform plan -auto-apply, terraform rm_
_terraform init, terraform run -auto-approve, terraform destroy_
_terraform init, terraform apply -auto-approve, terraform destroy_
_terraform import, terraform apply -y, terraform rm_

--> terraform init, terraform apply -auto-approve, terraform destroy
- its init for sure, then either plan X apply (no such thing as run) and it couldnt parse -auto-apply --> hence -auto-approve
- if run w/o ```terraform plan```, by default it would ask for approval (even for ```terraform apply```), but ```-auto-approve``` setting ... well approves w/o asking
- --> it used mostly in CI/CD where there is no one to write ```yes``` (says Gemini assistant)