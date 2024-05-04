import os

import pandas as pd
import sqlalchemy
from time import time
import argparse

GREEN_TRIPDATA_OUTPUT_DATA = 'green_tripdata.csv'
TAXI_ZONE_LOOKUP_OUTPUT_DATA = 'taxi_zone_lookup.csv'


def main(params: argparse.Namespace):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.database
    gzip_url = params.gzip_url
    csv_url = params.csv_url

    engine = connect_to_db(user, password, host, port, db)

    download_data_with_bash(gzip_url, GREEN_TRIPDATA_OUTPUT_DATA, gzip=True)
    download_data_with_bash(csv_url, TAXI_ZONE_LOOKUP_OUTPUT_DATA)

    process_green_trip_data(GREEN_TRIPDATA_OUTPUT_DATA, engine,
                            GREEN_TRIPDATA_OUTPUT_DATA.split('.')[0])
    process_taxi_zone_lookup_data(TAXI_ZONE_LOOKUP_OUTPUT_DATA, engine,
                                  TAXI_ZONE_LOOKUP_OUTPUT_DATA.split('.')[0])


def connect_to_db(user: str, password: str, host: str, port: str = '5432',
                  db: str = 'ny_taxi') -> sqlalchemy.engine:
    try:
        engine = sqlalchemy.create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    except Exception as e:
        print(f'something went wrong while connecting to DB ==> {e}')
        raise TimeoutError('ingestion operation terminated... cannot connect to DB')
    return engine


def process_green_trip_data(file_path: str, engine: sqlalchemy.engine, table_name: str):
    print("processing green trip data...")

    green_tripdata_df_iter = pd.read_csv(file_path,
                                         iterator=True,
                                         chunksize=1000000)
    if engine is not None:
        while True:
            t_start = time()
            df = next(green_tripdata_df_iter)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime, format='mixed')
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime, format='mixed')
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()

            print("inserted another chunk , took %.3f seconds" % (t_end - t_start))


def process_taxi_zone_lookup_data(file_path: str, engine: sqlalchemy.engine, table_name: str):
    print("processing taxi zone lookup data...")

    taxi_zone_df_iter = pd.read_csv(file_path,
                                    iterator=True,
                                    chunksize=1000000)
    if engine is not None:
        while True:
            t_start = time()
            df = next(taxi_zone_df_iter)
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()

            print("inserted another chunk , took %.3f seconds" % (t_end - t_start))


def download_data_with_bash(url: str, output_file: str, gzip: bool = False):
    print(f"downloading data into {output_file}")
    try:
        # subprocess.run(['wget', url, '-O', output_file])
        if gzip:
            print(f'wget -qO - {url} | gunzip > {output_file}')
            os.system(f'wget -qO - {url} | gunzip > {output_file}')
        else:
            if not os.path.exists(output_file):
                with open(output_file, 'w') as f:
                    f.write('')
            print(f'wget -qO - {url} | gunzip > {output_file}')
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
    parser.add_argument("--gzip_url", help="url of the csv file")
    parser.add_argument("--csv_url", help="url of the csv file")

    args = parser.parse_args()

    main(args)

# python3 ingest_data.py \
#     --user=postgres \
#     --password=postgres \
#     --port=5432 \
#     --host=localhost \
#     --database=ny_taxi \
#     --gzip_url='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz'
#     --csv_url='https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
