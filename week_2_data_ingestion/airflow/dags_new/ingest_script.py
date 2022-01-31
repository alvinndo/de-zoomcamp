import sys
import csv

from time import time

import pandas as pd
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)

    file = open(csv_file)
    reader = csv.reader(file)
    lines = len(list(reader))

    print("Begin large database ingestion...\n")

    try:
        print('Starting engine...')
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
        print("Engine connected\n")
    except:
        print("Something failed with the engine!\n", sys.exc_info()[0])

    chunksize = 100000

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=chunksize)

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