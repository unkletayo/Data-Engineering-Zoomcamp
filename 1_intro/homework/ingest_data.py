#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import gzip
import shutil
from time import time
import pandas as pd
from sqlalchemy import create_engine


def unzip_file(source_file, dest_file):
    """Unzips a .gz file."""
    with gzip.open(source_file, 'rb') as f_in:
        with open(dest_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def download_file(url, output_file):
    """Downloads a file if it does not already exist."""
    if os.path.exists(output_file):
        print(f'{output_file} already exists, skipping download.')
    else:
        print(f'Downloading {output_file}...')
        os.system(f"wget {url} -O {output_file}")


def main(params):
    """
    Downloads and unzips the NYC taxi trips data from the GitHub repo,
    and inserts it into a PostgreSQL database.

    Parameters
    ----------
    params : argparse.Namespace
        Parameters parsed from the command line, including the PostgreSQL
        credentials, host, port, database name, and table name.
    """
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name_1 = params.table_name_1
    table_name_2 = params.table_name_2
    
    # URLs and file names
    url1 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    url2 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    csv1_gz = 'trips.csv.gz'
    csv1 = 'trips.csv'
    csv2 = 'zones.csv'
    
    # Download the gzipped trip data
    download_file(url1, csv1_gz)
    
    # Unzip the file if it was downloaded
    if os.path.exists(csv1_gz):
        print(f'Unzipping {csv1_gz}...')
        unzip_file(csv1_gz, csv1)
        os.remove(csv1_gz)  # Remove the .gz file after extraction

    # Download the zones data
    download_file(url2, csv2)
    
    print('Finished downloading files')
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Trips
    df_iter = pd.read_csv(csv1, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name_1, con=engine, if_exists='replace')
    df.to_sql(name=table_name_1, con=engine, if_exists='append')
    
    for chunk in df_iter:
        t_start = time()
        df = chunk
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name_1, con=engine, if_exists='append')
        t_end = time()
        print(f'Inserted another chunk, took {t_end - t_start:.3f} seconds')
    
    print('Finished inserting trips to database')

    # Zones
    print('Inserting zones to database')
    df = pd.read_csv(csv2)
    df.to_sql(name=table_name_2, con=engine, if_exists='append')
    print('Finished inserting zones to database')


if __name__ == '__main__':
    parse = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parse.add_argument('--user', help='username for postgres')
    parse.add_argument('--password', help='password for postgres')
    parse.add_argument('--host', help='host for postgres')
    parse.add_argument('--port', help='port for postgres')
    parse.add_argument('--db', help='database name for postgres')
    parse.add_argument('--table_name_1', help='name of the table for trips')
    parse.add_argument('--table_name_2', help='name of the table for zones')
    
    args = parse.parse_args()
    
    main(args)
