import pandas as pd
import argparse
import os
import sys
import csv
from time import time
from sqlalchemy import create_engine

def download_csv(url, output):
    try:
        print(f"Searching {url} to download CSV...")
        if os.path.isfile(output):
            print(f'{output} already exists!\n')
        else:
            print(f"Downloading...")
            os.system(f"wget {url} -O {output}")
            print(f"Downloaded to {output} complete!\n")
    except:
        print("Oops! Something went wrong downloading the file!", sys.exc_info()[0])

def ingest(csvname, user, password, host, port, db, table_name):
    print(f'Grabbing {csvname} to start ingestion\n')

    file = open(csvname)
    reader = csv.reader(file)
    lines = len(list(reader))

    if lines > 1000000:
        try:
            print("Begin large database ingestion...\n")

            try:
                print('Starting engine...')
                engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
                print("Engine connected\n")
            except:
                print("Something failed with the engine!\n", sys.exc_info()[0])

            chunksize = 100000

            df_iter = pd.read_csv(csvname, iterator=True, chunksize=chunksize)

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            df.to_sql(name=table_name, con=engine, if_exists='append', method='multi')

            n = chunksize*2
            for chunk in df_iter:
                print(f"Starting chunk {n}/{lines}")
                t_start = time()
                
                df = chunk

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists='append', method='multi')
                
                t_end = time()
                print(f"Imported chunk... -- {t_end - t_start:.3f}s. -- {int( (n/lines)*100 )}% completed.")
                
                if lines - n < chunksize:
                    n = lines - chunksize

                n += chunksize
        except:
            print("Oops! Something went wrong with the ingestion!", sys.exc_info()[0])
    else:
        try:
            print("Begin zone database ingestion...\n")

            try:
                print('Starting engine...')
                engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
                print("Engine connected\n")

                df = pd.read_csv(csvname)

                df.to_sql(name='zones', con=engine, if_exists='replace')

                print("Successfully imported zones data\n")
            except:
                print("Something failed with the engine!\n", sys.exc_info()[0])
        except:
            print("Oops! Something went wrong with the ingestion!", sys.exc_info()[0])            

def main(params, zones):
    user = params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url = params.url
    output_csv=params.output_csv

    if zones == 'y':
        try:
            download_csv(zones_url, 'zones_output.csv')
            ingest('zones_output.csv', user, password, host, port, db, table_name)

            download_csv(url, output_csv)
            ingest(output_csv, user, password, host, port, db, table_name)
        except:
            print('Oops! Something went wrong in the main!', sys.exc_info()[0])
    else:
        try:
            download_csv(url, output_csv)
            ingest(output_csv, user, password, host, port, db, table_name)
        except:
            print('Oops! Something went wrong in the main!', sys.exc_info()[0])

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='username for PostgreSQL')
    parser.add_argument('--password', help='password for PostgreSQL')
    parser.add_argument('--host', help='hostname for PostgreSQL')
    parser.add_argument('--port', help='root for PostgreSQL')
    parser.add_argument('--db', help='database name for PostgreSQL')
    parser.add_argument('--table_name', help='table name for PostgreSQL')
    parser.add_argument('--url', help='URL of CSV file')
    parser.add_argument('--output_csv', help ='name of the .csv output file')

    args = parser.parse_args()
    hi= print()

    zones_input = input("Input taxi_zones data? (y/n): ")
    if zones_input.lower() == 'y':
        zones_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
        main(args, 'y')
    else:
        main(args, 'n')
