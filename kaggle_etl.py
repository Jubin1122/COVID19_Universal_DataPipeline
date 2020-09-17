import configparser
import psycopg2
from sql_queries import *
import pandas as pd
import requests
import json
import urllib.request
from pandas.io.json import json_normalize
import time
import boto3
import glob
import zipfile
import numpy as np
from aws.create_cluster import *
from data_cleaning import kaggle_usa_clean as k_usa
from data_cleaning import kaggle_world_clean as k_world
from sqlalchemy import create_engine


# clean and transform data, and load the data into staging table
def kaggle_load_staging_tables(conn, s3, USAFacts, worldometer): 
    print('Data Cleaning initited.....', '\n')
    USA_data  = k_usa(USAFacts)

    print('Loading to redshi9ft initiated.....', '\n')
    USA_data.to_sql('covid19_usa_data', conn, index=False, if_exists='append',chunksize= 1000, method='multi') 
    print('Moved to staging table', '\n')

    print('Data Cleaning initited.....', '\n')
    World_data = k_world(worldometer)

    print('Loading to redshi9ft initiated.....', '\n')
    World_data.to_sql('covid19_data', conn, index=False, if_exists='append',chunksize= 1000, method='multi') 
    print('Moved to staging table', '\n')

# Unzip the zip folder and transform the CSV to pandas dataframe
def process_staging_tables(conn, path, func):

    load_DWH_Params()
    s3 = s3_client()

    USAFacts, worldometer = ([] for i in range(2)) 
    #req_files = ['ECDC', 'New_York_Times', 'USAFacts', 'county_health_rankings', 'johns_hopkins_csse', 'world_bank', 'worldometer']
    with zipfile.ZipFile(path, "r") as f:
        for name in f.namelist():
            if name.endswith(".csv"):
                if 'USAFacts' in name: 
                    data = f.open(name)
                    USAFacts.append(pd.read_csv(data))
            

                elif 'worldometer' in name: 
                    data = f.open(name)
                    worldometer.append(pd.read_csv(data))
                    
        USAFacts = pd.concat(USAFacts, axis=0, ignore_index=True)
        worldometer = pd.concat(worldometer, axis=0, ignore_index=True)
    
    func(conn, s3, USAFacts, worldometer)

    
    print('files processed')

        
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = create_engine('postgresql://{}:{}@{}:{}/{}'.format(*config['CLUSTER_SQL'].values()))

    process_staging_tables(conn, path ="kaggle_covid19_data.zip", func=kaggle_load_staging_tables)

    


if __name__ == "__main__":
    main()