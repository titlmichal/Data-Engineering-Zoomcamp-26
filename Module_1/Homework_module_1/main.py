import pyarrow
import pandas as pd
from sqlalchemy import create_engine

def main():
    parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    pg_user = "root"
    pg_password = "root"
    pg_host = "localhost"
    pg_port = "5432"
    pg_db = "ny_taxi"

    # connecting to (already running docker postgredb container)
    engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

    # creating the tables
    pd.read_parquet(parquet_url).head(0).to_sql(
                    name="green_taxi_nov_25", 
                    con=engine, 
                    if_exists="replace"
                    )

    pd.read_csv(csv_url).head(0).to_sql(
                    name="taxi_zone_lookup", 
                    con=engine, 
                    if_exists="replace"
                    )

    #filling the tables (read_parquet doesnt support iterator)
    parquet_file = pd.read_parquet(parquet_url)
    parquet_file.to_sql(
                    name="green_taxi_nov_25", 
                    con=engine, 
                    if_exists="append"
                    )
    print("Parquet file ingested")

    csv_file = pd.read_csv(csv_url)
    csv_file.to_sql(name="taxi_zone_lookup", con=engine, if_exists="append")
    print("Csv file ingested")

if __name__ == "__main__":
    main()