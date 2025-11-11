import pandas as pd
import os
from sqlalchemy import create_engine
import time
import logging


logging.basicConfig(
    filename = "logs/ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

engine = create_engine(r'sqlite:///E:\Vendor Performance Analysis\inventory.db')

def ingest_db(df, table_name, engine):
    '''This function will ingest the dataframe into database table'''
    print(table_name)
    df.to_sql(table_name, con = engine, if_exists = "replace", index = False)
    
def load_raw_data():
    '''This function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    folder_path = r"E:\Vendor Performance Analysis\data"
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            chunk_size = 1000000  # adjust as per memory
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                logging.info(f'Ingesting, {file} in db')
                ingest_db(chunk,file[:-4],engine)
    end = time.time()
    total_time = (end-start)/60
    logging.info("-----------Ingestion Complete----------")
    logging.info(f'Total Time Taken: {total_time} minutes')
    
if __name__ == '__main__':
    load_raw_data()