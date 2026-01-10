import pandas as pd
import click
from tqdm import tqdm
from sqlalchemy import create_engine

# DATA TYPES AND COLUMNS FOR PARSING
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

# THIS ALLOWS FOR CONFIGURING SCRIPT PARAMS FROM THE CLI
@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-password', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--table-name', default=f'yellow_taxi_data', help='Target table name')

def run(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, chunksize, table_name):
    # year = 2021
    # month = 1

    # pg_user = "root"
    # pg_password = "root"
    # pg_host = "localhost"
    # pg_port = "5432"
    # pg_db = "ny_taxi"

    # table_name = "yellow_taxi_data"

    # chunksize = 100000

    # DATA DOWNLOAD
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
    # :02d to keep the format of 2 digits

    # ENGINE FOR DB CONNECTION
    engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

    # SHOWING SCHEMA USED BELOW IN .to_sql() METHOD
    # print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))

    # ACTUALLY GETTING DATA AND GETTING DATA IN CHUNKS FOR CHUNK LOADING --> ITERATOR
    df_iter = pd.read_csv(url, 
                    dtype=dtype, 
                    parse_dates=parse_dates,
                    iterator=True,
                    chunksize=chunksize)
    # GETS ME AN INTERATOR, NOT DF

    first = True
    # ACTUAL INGEST
    for chunk in tqdm(df_iter):
        # CREATING THE TABLE
        if first:
            # ACTUALLY CREATING THE TABLE
            chunk.head(0).to_sql(
                name=table_name, 
                con=engine, 
                if_exists="replace"
                )
            first = False

        print(len(chunk))
        chunk.to_sql(
            name=table_name, 
            con=engine, 
            if_exists="append"
            )
        print("Chunk inserted")

    print(f"Data ingest done for month {month} of year {year}.")

if __name__ == "__main__":
    run()