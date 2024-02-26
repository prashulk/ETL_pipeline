import os, argparse
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from time import time

class DataIngestor:

    def __init__(self, user, password, host, port, db, table_name, file_path):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db = db
        self.table_name = table_name
        self.file_path = file_path
        self.engine = self.create_engine()

    def create_engine(self):
        return create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')

    def ingest_data(self):
        raise NotImplementedError("Must implement own method")

class CSVIngestor(DataIngestor):  

    def ingest_data(self):
        df_iter = pd.read_csv(self.file_path, iterator=True, chunksize=100000)

        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.head(n=0).to_sql(name = self.table_name, con = self.engine, if_exists='replace')
        df.to_sql(name = self.table_name, con = self.engine, if_exists='append')

        while True:
            try:
                t_start = time()
                df = next(df_iter)
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
                df.to_sql(name=self.table_name, con=self.engine, if_exists='append')
                t_end = time()
                print('inserted another chunk, took %.2f seconds'%(t_end - t_start))
            except StopIteration:
                print("Ingestion procss completed in database")
                break

class ParquetIngestor(DataIngestor):

    def _map_column_names(self, df):
        column_mapping = {
            'Airport_fee': 'airport_fee'
        }
        df = df.rename(columns=column_mapping)
        return df

    def ingest_data(self):
        file = pq.ParquetFile(self.file_path)
        batches_iter = file.iter_batches(batch_size=100000)

        df = next(batches_iter).to_pandas()
        df = self._map_column_names(df)
        t_start = time()
        chunk_count = 1

        for batch in batches_iter:
            batch_df = batch.to_pandas()
            batch_df = self._map_column_names(batch_df)
            chunk_start = time()
            batch_df.to_sql(name=self.table_name, con=self.engine, if_exists='append')
            chunk_end = time()
            print(f'Inserted chunk {chunk_count}, took {chunk_end - chunk_start:.2f} seconds')
            chunk_count += 1
        t_end = time()
        print(f'Finished ingesting data into the PostgreSQL database. Total time taken: {t_end - t_start:.2f} seconds')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data into Postgres')
    parser.add_argument('--user', required=True, help='user name for Postgres')
    parser.add_argument('--password', required=True, help='password for Postgres')
    parser.add_argument('--host', required=True, help='host for Postgres')
    parser.add_argument('--port', required=True, help='port for Postgres')
    parser.add_argument('--db', required=True, help='database name for Postgres')
    parser.add_argument('--table_name', required=True, help='name of table where results will be written')
    parser.add_argument('--file_path', required=True, help='Path to data file')

    args = parser.parse_args()

    ingestor_classes = {
        '.csv': CSVIngestor,
        '.parquet':ParquetIngestor
    }

    file_extension = os.path.splitext(args.file_path)[-1].lower()
    ingestor_class = ingestor_classes.get(file_extension)
    if ingestor_class:
        ingestor = ingestor_class(args.user, args.password, args.host, args.port, args.db, args.table_name, args.file_path)
        ingestor.ingest_data()
    else:
        raise ValueError("Unsupported file format")
