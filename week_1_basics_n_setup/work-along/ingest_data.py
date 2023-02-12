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

    # Download the parquet file and output it as file_name
    file_name = 'output.parquet'
    os.system(f'wget {url} -O {file_name}')

    # Create a connection to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the parquet file into a pandas dataframe
    df = pd.read_parquet(file_name, engine='pyarrow')

    # Convert the datetime columns to datetime objects
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Write the dataframe to the database in batches
    total_rows = len(df)
    rows_inserted = 0
    file = pq.ParquetFile(file_name)

    for batch in file.iter_batches():
        df_iter = batch.to_pandas()
        df_iter.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        rows_inserted += len(df_iter)
        percent_done= round(rows_inserted / total_rows * 100, 1)
        print(f'\nInserted {len(df_iter)} more rows, {percent_done}% done')


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