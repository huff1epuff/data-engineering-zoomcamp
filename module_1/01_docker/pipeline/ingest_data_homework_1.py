#!/usr/bin/env python
# coding: utf-8
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import requests
from io import BytesIO

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
    "congestion_surcharge": "float64",
    "ehail_fee": "float64"  
}

parse_dates = [
    "lpep_pickup_datetime", 
    "lpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--target-table', default='green_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for processing')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    """Ingest NYC green taxi data into PostgreSQL database."""
    
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    
    click.echo(f"Downloading data from {url}...")
    
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # Download the parquet file
    response = requests.get(url)
    response.raise_for_status()
    
    # Read parquet file
    df = pd.read_parquet(BytesIO(response.content))
    
    click.echo(f"Downloaded {len(df)} rows")
    
    # Process in chunks
    total_chunks = (len(df) + chunksize - 1) // chunksize
    
    first = True
    for i in tqdm(range(0, len(df), chunksize), total=total_chunks):
        df_chunk = df.iloc[i:i+chunksize].copy()
        
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace',
                index=False
            )
            first = False
        
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append',
            index=False
        )
    
    click.echo(f"Successfully ingested {len(df)} rows into {target_table}")

if __name__ == '__main__':
    run()