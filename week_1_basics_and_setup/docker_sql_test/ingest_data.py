
import pandas as pd
import argparse
import os
from time import time
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)


    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    while True:
        t_start = time()
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df = next(df_iter)
        
        t_end = time()
        
        print(f"importing chunk... took {t_end-t_start}s")

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='username for PostgreSQL')
    parser.add_argument('--password', help='password for PostgreSQL')
    parser.add_argument('--host', help='hostname for PostgreSQL')
    parser.add_argument('--port', help='root for PostgreSQL')
    parser.add_argument('--db', help='database name for PostgreSQL')
    parser.add_argument('--table_name', help='table name for PostgreSQL')
    parser.add_argument('--url', help='URL of CSV file')

    args = parser.parse_args()

    main(args)
