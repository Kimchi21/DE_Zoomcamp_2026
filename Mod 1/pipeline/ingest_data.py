#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
from pathlib import Path

DTYPE = {
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
    "congestion_surcharge": "float64",
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--year", default=2021, type=int, help="Year of the data")
@click.option("--month", default=1, type=int, help="Month of the data")
@click.option("--target-table", required=True, help="Target table name")
@click.option("--chunksize", default=100000, type=int, help="Chunk size")
@click.option(
    "--file",
    default=None,
    help="Path to local CSV file (inside container)",
)
def run(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    year,
    month,
    target_table,
    chunksize,
    file,
):
    """Ingest NYC Taxi data into PostgreSQL"""

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # Decide data source
    if file:
        csv_path = Path(file)
        if not csv_path.exists():
            raise FileNotFoundError(f"File not found: {csv_path}")

        print(f"📂 Reading local file: {csv_path}")

        df_iter = pd.read_csv(
            csv_path,
            iterator=True,
            chunksize=chunksize,
        )
    else:
        prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
        url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

        print(f"🌐 Downloading data from: {url}")

        df_iter = pd.read_csv(
            url,
            dtype=DTYPE,
            parse_dates=PARSE_DATES,
            iterator=True,
            chunksize=chunksize,
        )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
        )


if __name__ == "__main__":
    run()