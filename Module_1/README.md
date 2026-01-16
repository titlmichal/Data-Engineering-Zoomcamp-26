
# Module 1: docker, postgre, GCP, terraform

## Docker and postgres (workshop video)

https://www.youtube.com/watch?v=lP8xXebHmuE&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=11

https://github.com/alexeygrigorev/workshops/tree/main/dezoomcamp-docker

#### Back to docker:

- docker, containers, running postgresql in container, ingestion into postgresql, pgadming, docker networking and port mapping, docker compose, data ingestion pipeline, best practices for container DE workflows
- docker = containerazitaion SW (similar to VMs but leaner)
- docker image = snapshot of container defining how our app/pipeline is runy
- --> reproducibility in any envi, isolation = independent app runs, portability = run where docker is installed
- used in: integration tests (ci/cd), cloud pipeline (aws batch, kubernetes jobs), spark, serverless (lambda)
- btw github codespaces: accessible via repo (under green code button) --> creates safe dev cloud-based envi (workshop is using that)

#### Basic commands:

```bash
docker --version
```

```bash
docker run hello-world
```
*btw was failing at first bcs I was logged off

```bash
docker run -it ubuntu
```
- it for interactive terminal or something
- runs ubuntu container (isolated from my machine)
- container is stateless = after closing it, data is lost

```bash
docker run -it python:3.13.11
```
- python is the docker image name, 3.13.11 is the tag

- but I might want to have a bash session in this image (e.g. select given python version) --> entrypoint
```bash
docker run -it --entrypoint=bash python:3.13.11
```

```bash
docker ps -a
```
- ps shows running containers, -a shows stopped containers
- we can resume exited containers, BUT not good thing to do (antipattern)

```bash
docker ps -aq
```
- gives me IDs of exited containers

```bash
docker rm $(docker ps -aq)
```
- removes the exited containers by IDs
- course used backticks instead of dollarsign and brackers, but that didnt work for me

#### How to preserve state?

```bash
new-item one.txt
echo "Hello from me" > one.txt
```
- to create testing file(s) and content there
- when we want to access the files with docker? or use script accessing those files within docker container?
- --> using *volumes* => saying what in main machine is available to the container too
- --> maping the volume via -v
- BTW powershell defaultly stores txt as utf-16, not utf-8 (which docker expects) --> set correctly next time or change later

```bash
docker run -it --entrypoint=bash -v "$(pwd)\docker_test_dir:/app/test" python:3.13.11
```
- the ```"$(pwd)\docker_test_dir"``` is location in host machine (windows hence ```\```)
- then ```:```
- and then the location in container ```/app/test``` (linux/docker hence ```/```)
- this target container folder doesnt have to exist
- --> left side is local : right side is container
- this is MOUNTING my path to container --> *local machine and container talk to each other live!*
- --> now the script is run with the local files bcs the container *SEES THEM (!)*

#### Virtual envis and pipelines

- we want to take data from somewhere, do some magic and push somewhere = pipeline
- and we can do the magic in container (bcs then we can use e.g. cloud for that)
- this time: CSV --> pipeline --> PostgreSQL DB

We wrote this but dont want to install pyarrow and fastparquet locally
```python
import sys
import pandas as pd

print("helo pipeline; args: ", sys.argv)

# pipelines usually use parameters: e.g. which month to process
month = int(sys.argv[1])
print(f"Running pipeline for month {month}")

df = pd.DataFrame({"day": [1, 2], "passengers": [3, 4]})
df["month"] = month
print(df.head())

# parquet is binary file optimized for data 
# (will not run bcs I dont have pyarrow and fastparquet --> docker bcs i dont want it in my local machine, for demenstration purposes)
df.to_parquet(f"output_{month}.parquet")
```
- --> lets use virtual envi
- --> using uv for managing the virtual envis
```bash
pip install uv
```
- i used gemini recommended way bcs pip way didnt account for path
```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
```bash
uv init --python=3.13
```
- initializes whole python project (readme and mainly pyproject.toml and .python-version)
- pyproject for dependencies, python-version for fixing python version, .venv dir for lib versions

```bash
uv run where python
```
- tells me where is the python (linux uses ```which``` instead of ```where``` in windows)
- the python in this envi, not global one

```bash
uv run python -V
```
- tells me the version of envi python
- ```python -V``` tells me the global one

- we want to use the virtual envi for those dependencies
```bash
uv add pandas pyarrow
```
- adding pandas too bcs the envi doesnt have it
- and now the dependencies are *AUTOMATICALY* in the pyproject.toml

- now need to tell the pipeliny.py *which python to use*
- --> ctrl+p within the .py file --> ">Python: Select interpreter" --> find the one in the .env dir
- --> *now the script goes through bcs the virtual envi python HAS the needed dependencies*

#### Now putting this into docker

- lets create docker image according to our dependencies of virtual envi
- --> Dockerfile (just create new file of that EXACT name) --> tells docker how to build docker container
- 1st line always ```FROM name_of_base_image```
- then in this case we need the dependencies hence ```RUN pip install pandas pyarrow```
- then setting working dir (best practice - it will be created and automatically used in later commands) ```WORKDIR /code```
- then copying the script to current workdir (the code dir) ```COPY pipeline.py .```
- (the dot is there destination = current dir)
- and now need to build it --> to have among usable images
- plus once its build its already like this forever (e.g. no new pandas version will be used there, only the one when built)

```bash
docker build -t image_name:image_tag .
```
- the dot is again the current folder --> docker searches pwd for the dockerfile

```bash
docker run -it --entrypoint=bash zoomcamp:test_1
```
- now I can run it the container
- and then run the actual pipeline script (manually but i can add the ENTRYPOINT command to run it automatically)
- ```ENTRYPOINT ["python", "pipeline.py"]```
- (bcs the old test_1 was built without ENTRYPOINT, I built test_2 and test_3 and test_4)
- btw bcs its run in docker container, results (e.g. the parquet) stay there

```bash
docker run -it --entrypoint=bash zoomcamp:test_4 12
```
- im putting the 12 there as an argument for the script
- when it wasnt ran automatically, i was putting the arg there myself
- --> just DOCKERIZED the pipeline (this dockerfile doesnt deal with paths --> has to be in one folder)

- but what if I want to use uv instead of pip? 
- --> copy uv binary from official uv image (multi-stage build pattern)
- it uses --from argument and /uv as source and /bin/ as target
```COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/```
- --> then copy data from the virtual env to prepare it for install
```COPY "pyproject.toml" "uv.lock" ".python-version" ./```
- --> and then install dependencies from lock file (keeps reporducibility)
```RUN uv sync --locked```
- --> and change entrypoints to run with uv
```ENTRYPOINT ["uv", "run", "python", "pipeline.py"]```
- alternatively I can put ```ENV PATH="/app/.venv/bin:$PATH"``` there --> sets virtual env to path to use installed libs
- ... and keeps the entry points same without uv and run

#### Now lets run Postgre with Docker w/o installing it
```bash
docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi:/var/lib/postgresql \
  -p 5432:5432 \
  postgres:18
```
- no installation of Postgre needed but few envi vars are --> see the ```-e``` args
- these ```-e``` are app config args --> in this case it user, password and DB
- ```-v``` is here to map volume (in this case it will be docker internal bcs the host path is not mine)
- (but i could use ```$(pwd)/ny_taxi_postgres_data``` to have it locally saved)
- ```-p``` (port mapping) tells at which port the application in container is waiting for inputs and via which port in local host I can access that (port_of_host:port_of_container)
- ... so this port mapping is similar to volume mapping = say what on host machine is what in the container
- btw, obviously in VSC terminal I have to run it as one line w/o backslashes

- --> when ran, the DB itself is RUNNING now
```bash docker ps``` reveals its running

- now how to get to the DB? use pgcli tool
```uv add --dev pgcli```
- ```--dev``` tells its dev dependency --> will be to respective [dependency-groups] dependency group in pyproject.toml --> ```RUN uv sync --locked``` only installs production dependencies

- now lets use pgcli to connect to my postgre db
```uv run pgcli -h localhost -p 5432 -u root -d ny_taxi```
- ```uv run pgcli``` is standard, ```-h localhost``` bcs its running locally BUT thanks for mapping --> connects to docker container
- the ```-p 5432``` is for port, ```-u root``` as username and ```-d ny_taxi``` as DB name
- --> after running this, its prompts for password (set above) ... btw typing is not visible in terminal

- some useful postgre commands
```SQL
-- List tables
\dt
-- Create a test table
CREATE TABLE test (id INTEGER, name VARCHAR(50));
-- Insert data
INSERT INTO test VALUES (1, 'Hello Docker');
-- Query data
SELECT * FROM test;
-- Exit
\q
```

#### Ingesting data into postgre from a pipeline

- to add jupyter (dev dependency) to create a script for getting and transforming
```uv add --dev jupyter```
```uv run jupyter notebook```
- will run the notebook and open the jupyter envi at localhost (http://localhost:8888/tree) --> e.g. see in browser (cli shows the links too)
- if I was using codespaces, I (or VSC/github) would need to forward to different port (e.g. 8889)
- btw if was mapping ports, jup notebooks have default 8888 (postgre has 5432) so I could sort of only select the left side (my side)

```python
import pandas as pd
pd.__file__
``` 
--> shows something like pipeline\\.venv\\Lib\\site-packages\\pandas\\__init__.py' --> pandas are coming from virtual envi --> all other dependencies are there and the jup is running in venv too

- now within the notebook, lets read the data
```python
prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
url = f"{prefix}/yellow_tripdata_2021-01.csv.gz"
pd.read_csv(url)
```
- originally, the source was csv, but then moved to parquet --> but the workshop uses original csv they saved to show saving to parquet
- there are some problems with data types: vendor id being float and timestamps being objects (bcs csv does NOT contain schema, BUT parquet does --> has types)

- hence setting up the types via dtype variable (source: data dictionary from source) and parse_dates during reading the csv
```python
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

pd.read_csv(url, dtype=dtype, parse_dates=parse_dates)
```

- now lets put it into the postgre db
- first need some more libs to do so (via terminal like below or via notebook ```!uv add sqlalchemy```)
```uv add sqlalchemy psycopg2-binary```

- first I need to create an engine for connecting to the DB
- it has the info about the DB connection details: first say its postgresql :// then user name : then password @ its running on localhost : the port 5432 / and the DB name
- for that I need the psycopg2 (see above)
```python
from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
```
- (!) I had an issue of installing non-binary psychopg2, then installing binary and removing non-binary --> it remove a bit more --> sqlalchemy (which imports psycopg2) couldnt find it --> had to remove from basic cmd and readd with uv --> runs ok now

- DB needs schema --> lets create it with pd
```python
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
```
- retunrs this SQL --> it will be used by SQLalchemy when connecting to db to create the table
```SQL
CREATE TABLE yellow_taxi_data (
	"VendorID" BIGINT, 
	tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
	tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
	passenger_count BIGINT, 
	trip_distance FLOAT(53), 
	"RatecodeID" BIGINT, 
	store_and_fwd_flag TEXT, 
	"PULocationID" BIGINT, 
	"DOLocationID" BIGINT, 
	payment_type BIGINT, 
	fare_amount FLOAT(53), 
	extra FLOAT(53), 
	mta_tax FLOAT(53), 
	tip_amount FLOAT(53), 
	tolls_amount FLOAT(53), 
	improvement_surcharge FLOAT(53), 
	total_amount FLOAT(53), 
	congestion_surcharge FLOAT(53)
)
```

- creating empty table --> hence the head(0) --> uses the schema w/o any data
```python
df.head(0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")
```
- --> now when i check the DB via pgcli (and respective docker container running), the table is there!

- the csv is just too big --> cant load at once with RAM --> splitting the data into same size chunks --> inserting chunk by chunk
```python
df_iter = pd.read_csv(url, 
                 dtype=dtype, 
                 parse_dates=parse_dates,
                iterator=True,
                chunksize=100000)
```
- --> returns iterator (not df) to use for chunk loading --> next() function or for loop

- now the actual insert via loop
- (i added tqdm to see progress ```uv add tqdm```)
```python
from tqdm.auto import tqdm
for chunk in tqdm(df_iter):
    print(len(chunk))
    chunk.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")
    print("Chunk inserted")
```
- --> when I connect to db with pgcli I can query the table and see it filling already (whole run took like 2 mins)

- now I want to create a script from the notebook
```uv run jupyter nbconvert --to=script notebook.ipynb```
- creates a notebook.py from notebook.ipynb (--> changing to ingest_data.py + doing some changes bcs its a script, duh)
- ```uv run python .\ingest_data.py``` to test it

- now I want the parameters within the script to be configurable through cli (--> to use it in docker later)
- e.g. ```uv run python .\ingest_data.py --year=2022```
- --> will use click library to do so --> ```uv add click```
- adding the code below and adjusting the main function
```python
@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
```
- --> now I can do something like ```uv run python .\ingest_data.py --year 2020 --month 7```

#### Now I want to put this pipeline into docker too

- --> just switch from ```pipeline.py``` to ```ingest_data.py``` in the dockerfile
- + I had to change the PATH to reference ```/code``` not ```/app``` (as the example from workshop)
- ... and build it ```docker build -t zoomcamp:data_ingest_1 .```
- now I can run it as a container:
```bash
docker run -it   zoomcamp:data_ingest_1     --pg-user=root     --pg-password=root     --pg-host=localhost     --pg-port=5432     --pg-db=ny_taxi     --table-name=yellow_taxi_trips_2021_2     --year=2021     --month=2     --chunksize=100000
```
- so instead of ```uv run python ... ``` im just switching to running the container ```docker run -it ...``` whichs image runs the script
- BTW within the script and click functions I initially used e.g. ```'--pg_password'``` as first arg --> click expects the same in cli but for other script args i used e.g.  ```'--pg-host'``` --> click is expecting underscore for password but not for host --> have to have one single approach --> ```'--pg-password'``` --> then I can write the same in cli
- BUT this docker run will FAIL bcs DB is running in hostmachine local host AND the script is referencing to the hostmachine within the container as the container sees itself as a local host --> no DB there

- SOLUTION: docker network
```bash
docker network create pg-network
```
- --> now I want to tell both containers to use this network
- the network name is argument hence the equal sign, name is being set hence no equal sign
- 1st the db
```bash
docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdb
  postgres:18
```
- 2nd the script (not setting name bcs it doenst need to be discovered by other things in the network)
- + host is now the pgdb, not localhost
```bash
docker run -it  --network=pg-network zoomcamp:data_ingest_1     --pg-user=root     --pg-password=root     --pg-host=pgdb     --pg-port=5432     --pg-db=ny_taxi     --table-name=yellow_taxi_trips_2021_2     --year=2021     --month=2     --chunksize=100000
```
- and now I can run the pgcli to check the DB itself
```uv run pgcli -h localhost -p 5432 -u root -d ny_taxi```
- its still localhost bcs its port 5432 to which the containers are mapped --> my machine is connecting into the pgdb network via this port
- --> containers are contacting each other via names X my host machine via localhost (if the ports are mapped)

- BUT nice than pgcli is pgadmin = visual interface
- --> needs some envi vars too, volume mapping as well, port mapping, the network setup, name of the container in network and name of the image
- inside port is 80, but outside is 8085 (80 is default, but dont want to make mess on this host machine) --> my machines = 8085 : container = 80
```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```
- --> then going to ```http://localhost:8085``` in browser and configuring with the values used to start (this container and the db one)
- configuration: right on servers --> registrer --> server --> (general tab) name = local docker --> (connection tab) host name = pgdb, port = 5432, username = root, password = root

- but what if I dont want to paste the commands everytime and want to run it all at once? --> Docker compose
- --> create new file "docker-compose.yaml" = describes all the images that need to be run (pgdb, pgadmin in thic case)
- btw by default what is run via/in one docker compose file is run within ONE network
- how to make one? put the single commands together and ask GPT/Gemini :)
- result:
```yaml
services:
  pgdb:
    image: postgres:18
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "ny_taxi:/var/lib/postgresql:rw"
    ports:
      - "5432:5432"
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    volumes:
      - "pgadmin_data:/var/lib/pgadmin"
    ports:
      - "8085:80"

volumes:
  ny_taxi:
  pgadmin_data:
```
- how is is structured:
1) level 1: services for as containers + volumes as volumes
2) level 2: name of each container (NOT image name) --> pgdb and pgadmin (btw then the actual container name is dir_name-this_name-number)
3) level 3: image name (NOT container name), environment variables, volumes and port
+ volumes are declared twice
1) once below under volumes --> im using NAMED volumes (serviced by Docker itself), im NOT doing bind mount (exact path on my host machine) => its telling Docker that there actually IS or TO CREATE a volume with this name
2) once within services = thanks to being declared under volumes, Docker knows its a volume, not a path in local machine (!)
- generally: named volumes better for DB and safety (quicker but harder to find --> ```docker volume inspect```) X bind mounts better for scripts and development (can easily edit but troublesome with access rights)

```docker-compose up```
- I have to be in the respective directory --> it will run the containers according to the setup
- --> bcs Im using docker COMPOSE now, the data is not there as before BCS the volumes are different
- --> COMPOSE add directory name in front of the volume name --> hence ny_taxi volume is actually pipeline_ny_taxi --> see ```docker volume ls```
- --> to get data, rerun the ingestion script and edit the network to network created by docker compose, visible by ```docker network ls```
```bash
docker run -it  --network=pipeline_default zoomcamp:data_ingest_1     --pg-user=root     --pg-password=root     --pg-host=pgdb     --pg-port=5432     --pg-db=ny_taxi     --table-name=yellow_taxi_trips_2021_2     --year=2021     --month=2     --chunksize=100000
```

- BTW I made a mistake that I changed service name in readme (from database to pgdb) but not in yaml --> pgadmin couldnt find DB
- --> service name/key in yaml = hostname in network/DNS

```docker-compose down```
- to stop to containers
```docker-compose ps```
- to check what is running

#### Clean up

```docker container prune``` to remove all stopped containers
```docker image prune``` to remove all images
```docker volume prune``` to remove all images
```docker network prune``` to remove all images

## SQL Refresher (course video)

https://www.youtube.com/watch?v=QEcps_iskgg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=11
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/docker-sql/09-sql-refresher.md

- nothing special except maybe few I dont use that often
1) implicit inner join = putting all source table unders FROM clause and setting join conditions under WHERE
```SQL
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
    CONCAT(zdo."Borough", ' | ', zdo."Zone") AS "dropoff_loc"
FROM
    yellow_taxi_trips t,
    zones zpu,
    zones zdo
WHERE
    t."PULocationID" = zpu."LocationID"
    AND t."DOLocationID" = zdo."LocationID"
LIMIT 100;
```
2) full outer join + filtering for rows with null values in key columns = other way to do minus


## Intro to GCP (course video)

https://www.youtube.com/watch?v=18jIzE41fJ4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=4
https://console.cloud.google.com/

- cloud computing service
- multiple categories:
![alt text](image.png)
- here mainly big data (bigquery, dataflows, dataprep, dataproc) and storage/DBs (bigtable, datastore, spanner, sql, storage)

- GCP works in terms in projects mainly
- navgigation via: 1) navigation menu on the left; 2) search; 3) tiles on the main menu
- now seeing cloud storage --> contains buckets (buckets of data) 
- ... similar to S3 = buckets contain files (so kinda like a directory)
- ... into bucket, we will load data during the course

## Introduction Terraform: Concepts and Overview, a primer (course video)

https://www.youtube.com/watch?v=s2bOYDCKl_M&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=12
- allegedly slight intro into Terraform, enough to get me going/setting up infra and resources
CAUTION: be very sure of what Im deploying, before deploying! (it can cost me)

- terraform is about terraforming cloud (or local) plaftorm = setting up infrastructure so place where the code can live and SW can run
- = IaaS tool letting me to define cloud or on-prem resources in readable config files that can be versioned, reused and shared
- ... it comes down to the IaaS = infrastructure as a code
1) simplicity = easy to keep track of the file containing it all (terraform file, .ts suffix)
2) collaboration = pushable to repo, easier to review, correct, ... and deploy the infra
3) reproducibility = e.g. when sharing interesting project to be ran --> just update params and go
4) ensure resources are removed = once im done with resources (and dont want to keep paying) --> quick terraform command --> everything is down

### what it is NOT/what it does NOT do?
1) manage/update code in infrastructure
2) give ability to change immutable resources (e.g. virtual machine type, google cloud/bucket location)
3) manage resources not defined in the terraform file (e.g. kubernetes cluster)

- Terraform stands in my local machine as a communicator with platforms of services (GCP, Azure, AWS)
- --> I define in terraform file I want to use e.g. GCP --> terraform will use its file and connect to that platform
- --> I will need some way to authorize the access (service account, access token, ...)

https://registry.terraform.io/browse/providers
- what are providers?
- = logical abstraction of an upstream API. They are responsible for understanding API interactions and exposing resources.
- code allowing terraform to communicate to manage resources on AWS/Azure/GCP/Kubernets/VSphere/Alibaba cloud/AD...
- ... code to communicate with cloud/on-prem platforms

### key terraform commands
1) ```terraform init``` --> to get providers defined in the file
2) ```terraform plan``` --> what am I about to do/which resources will be created
3) ```terraform apply``` --> do what is defined/planned in the terraform file/go out and build the infra
4) ```terraform destroy``` --> remove everything define in terraform file/destroys everthing built

## Terraform Basics: Simple one file Terraform Deployment (course video)

https://www.youtube.com/watch?v=Y2ux7gq3Z0o&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=13

https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/terraform/1_terraform_overview.md

- 1st setting up service account to be used as authentication to GCP
- service account is similar to standard account but not meant for logging in by user, but by code to be used for running stuff
- IAM --> service accounts
- name is not that important, BUT role is --> i want to create and use bucket AND create dataset --> 2 roles
--> service = storage; roles = storage admin
--> service = bigquery; roles = bigquery admin
- in real world I would want to limit the roles, but this is about setting up and taking down resources --> bucket creater and destruction in storage + create and destroy dataset in bigquery
- BTW permissions/roles can be added/removed to each account in IAM settings

### Service account

- --> now I need a way to say that I have the permission to run as the service account --> key --> ```Manage keys``` under given account actions
- BTW by default its restricted now --> docs approach didnt work --> had to set org policy admin to my role + go to cloud console:
```gcloud resource-manager org-policies disable-enforce iam.disableServiceAccountKeyCreation --project=terraform-demo-484316```
- CAUTION: NEVER EVER SHARE THIS (e.g. to github or email or drive or anyone)...same like with SSH, private keys and similar
--> if put in watched directory --> use gitignore

- Terraform VSC extension: just download the one from the author company: Hashicorp
- helps with autocomplete and this good stuff

### terraform file

- install terraform according to this: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/terraform/windows.md
- add terraform to PATH: https://gist.github.com/nex3/c395b2f8fd4b02068be37c961301caa7 (advanced system settings - envi variables - add path to the terraform.exe directory)
- create ```main.tf```
- start with provider setuo from docs: https://registry.terraform.io/providers/hashicorp/google/latest/docs
```
terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = "./terraform_creds.json"
  project     = "terraform-demo-484316"
  region      = "europe-central2"
}
```
- btw ```terraform fmt``` to format the file (indentens and stuff)
- btw region is choosen by me --> europe-central2 = should be Warsaw
- terraform will look for credentials file with certain name resembling credentials but to be safe: credentials = ```".\terraform_creds.json"```

#### Init

- ```terraform init``` = gets me provider (= the code needed for terrafrom to connect and talk to GCP) ... sorts of the path/door to GCP
- --> created a file called ```.terraform.lock.hcl```

- now I want to create/use a bucket in GCP storage: https://registry.terraform.io/providers/hashicorp/google/4.72.1/docs/resources/storage_bucket
```
resource "google_storage_bucket" "auto-expire" {
  name          = "terraform-demo-484316-terra-bucket"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
```
- after ```resource``` goes the name - important --> WHAT KIND of RESOURCE to ceate
- after that its VARIABLE name ("demo-bucket"), local (not needed to be globally unique)  --> to distinguish between them --> then will be referred to as ```google_storage_bucket.demo-bucket```
- ```name          = "terraform-demo-484316-terra-bucket"``` needs to be unique across ALL GCP --> usually project name is unique
- ```lifecycle_rule``` one of possible configurations blocs (this type ```type = "AbortIncompleteMultipartUpload"``` specifies that if partitioned/chunk load files, previously uploaded data will be deleted, bcs they could be useless)
- ```age``` of lifecycle is in days (see docs) --> here if the load didnt finish in day => remove uploaded data

#### Plan

- ```terraform plan``` = terraform will tell me whats about to do and details of those actions (e.g. create bucker: id, location, name, url, ...)
- btw I was having issues with the credentials bcs I used ```".\terraform_creds.json"``` instead of ```"./terraform_creds.json"``` bcs HCL (terraform langugae) understands ```\``` as escape character (in this case, tf understood its json file bcs of the ```.```)

#### Apply (this is considered deployment)

- ```terraform apply``` = deploys the infra according the plan
- file ```terraform.tfstate``` (and ```terraform.tfstate.lock.info``` temporarily) is created 
- I had to put this into the tf file in bucket resource section ```uniform_bucket_level_access = true``` bcs GCP now requires that
- uniform bucket level access = data is accessed via GCP IAM (= roles), not for each file --> easier to audit and prevent leaks (e.g. when I would leave some file public)
- and now the bucket IS THERE

- now whats in now the difference in ```.tfstate file?```
- well, what cmd wrote: google_storage_bucket.demo-bucket: Creation complete after 3s [id=terraform-demo-484316-terra-bucket]
- --> there is a new bucket on GCP terraform knows as "demo-bucket" ... and many other info

#### Destroy

- ```terraform destroy``` = deletes everything created, according to the tf file and tfstate file 
- now the tfstate is almost empty, just some basic and ```"resources": []```
- BUT there is now ```terraform.tfstate.backup``` --> it knows the config history

- next video: bigger project, variables, abstract values from variable file (to be reused)

#### gitignore

- find it and use it!

#### SUMMARY

- tf is state machine = uses tf file to do stuff BUT important is .tfstate file
- tfstate stores history --> do NOT change manually
- workflow: init (gets providers) --> plan (shows plan, compares tfstate and realy) --> apply (do stuff and change reality)
- provider = binary code read by HCL that sends API (REST) to google (in this case)
- upcoming: variables.tf to make it nice and usable for dev, test and prod envis

## Terraform variables (course video)

https://www.youtube.com/watch?v=PBi0hHjLftk&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=14

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/terraform/terraform

- ...

...stopped at 00:01/24:09