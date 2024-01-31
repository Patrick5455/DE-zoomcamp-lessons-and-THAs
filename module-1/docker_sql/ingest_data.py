import os

import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import requests
import subprocess


def main(params: argparse.Namespace):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.database
    table_name = params.table
    url = params.url

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    output_data = 'output.csv'

    download_data_with_bash(url, output_data)

    df_iter = pd.read_csv(output_data,
                          iterator=True,
                          chunksize=1000000)

    while True:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime, format='mixed')
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime, format='mixed')
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()

        print("inserted another chunk , took %.3f seconds" % (t_end - t_start))


def download_data_with_requests(url: str, output_file: str):
    print("downloading data")
    try:
        r = requests.get(url, allow_redirects=True)
        open(output_file, 'wb').write(r.content)
        print("downloaded data")
    except Exception as e:
        print(e)


def download_data_with_bash(url: str, output_file: str):
    print("downloading data")
    try:
        if os.path.exists(output_file):
            print("file already exists")
            return
        else:
            with open(output_file, 'w') as f:
                f.write('')
            # subprocess.run(['wget', url, '-O', output_file])
            os.system(f'wget {url} -O {output_file}')
            print("downloaded data")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data into Postgres')

    print("collecting arguments")

    parser.add_argument("--user", help="Postgres user")
    parser.add_argument("--password", help="Postgres password")
    parser.add_argument("--host", help="Postgres host")
    parser.add_argument("--port", help="Postgres port")
    parser.add_argument("--database", help="Postgres database")
    parser.add_argument("--table", help="name of the table where we would write the results to")
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()

    main(args)

# python3 ingest_data.py \
#     --user=postgres \
#     --password=postgres \
#     --port=5432 \
#     --host=localhost \
#     --database=ny_taxi \
#     --table=yellow_taxi_trips \
#     --url='https://data.cityofnewyork.us/api/views/kxp8-n2sj/rows.csv?accessType=DOWNLOAD'
