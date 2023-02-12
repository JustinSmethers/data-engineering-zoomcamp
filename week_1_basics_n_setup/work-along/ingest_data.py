import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Get the file name from the url
    if '.parquet' in url.lower():
        file_name = 'output.parquet'
        is_parquet = True
    elif '.csv' in url.lower():
        file_name = 'output.csv'
        is_parquet = False
    else:
        raise ValueError('File must be either .parquet or .csv')
    
    # Download the file from the url
    os.system(f'wget {url} -O {file_name}')

    # Create a connection to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    if is_parquet:
        file = pq.ParquetFile(file_name)
        # Iterate through the parquet file in batches
        for batch in file.iter_batches():
            # Convert the batch to a pandas dataframe
            df = batch.to_pandas()
            # Convert the datetime columns to datetime objects
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            # Insert the data into the database
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

            print(f'\nInserted {len(df)} rows into {table_name}')

    else:
        # Iterate through the csv file in batches
        df = pd.read_csv(file_name)
        rows_inserted = 0
        # Insert the data into the database
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        rows_inserted += len(df)

        print(f'\nInserted {len(df)} rows into {table_name}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user', help='Postgres user')
    parser.add_argument('--password', help='Postgres password')
    parser.add_argument('--host', help='Postgres host')
    parser.add_argument('--port', help='Postgres port')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table to insert data into')
    parser.add_argument('--url', help='url of parquet file to ingest')

    args = parser.parse_args()

    main(args)