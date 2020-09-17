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
#from create_cluster import main
from aws.create_cluster import *
from data_cleaning import s3jhu_data_clean as s3_jhu
from data_cleaning import s3rearc_covid_19_test_clean as s3_covid_usa
from data_cleaning import s3rearc_covid_19_world_test_clean as s3_covid_world
import os
from sqlalchemy import create_engine

# Load json data from AWS bucket, clean and transform data, and load the data into staging table
def aws_load_staging_table(conn, s3, buck_name, dire_path):

    # Downloading files for AWS S3 bucket
    my_bucket = s3.Bucket(buck_name)
    for f in dire_path:
        for file in my_bucket.objects.filter(Prefix= f):
            if not os.path.exists(os.path.dirname(file.key)):
                os.makedirs(os.path.dirname(file.key))
            my_bucket.download_file(file.key,file.key)
    

    all_files = []
    for root,dir,files in os.walk('/Nano_Degree_Capstone_Project/'):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            if 'VSWorkspaceState' not in f:
                all_files.append(os.path.abspath(f))    
    
    eng_jhu, rearc_covid_19_test, rearc_covid_19_world_test = conv_dataframe(all_files)

    
    print('Data Cleaning initiated......', '\n')
    # Data cleaning and transformation
    eng_jhu_proc =  s3_jhu(eng_jhu)

    print('Loading to redshi9ft initiated.....', '\n')
    #SQL Alchemy bulk insert directly to the staging table
    eng_jhu_proc.to_sql('covid19_data', conn, index=False, if_exists='append',chunksize= 1000, method='multi') 
    print('Moved to staging table', '\n')

    print('Data Cleaning initiated......', '\n')
    # Data cleaning and transformation
    rearc_covid_19_test_proc = s3_covid_usa(rearc_covid_19_test)

    print('Loading to redshi9ft initiated.....', '\n')
    #SQL Alchemy bulk insert directly to the staging table
    rearc_covid_19_test_proc.to_sql('covid19_usa_data', conn, index=False, if_exists='append',chunksize= 1000, method='multi') 
    print('Moved to staging table', '\n')

    print('Data Cleaning initiated......', '\n')
      # Data cleaning and transformation
    rearc_covid_19_world_test_proc = s3_covid_world(rearc_covid_19_world_test)

    print('Loading to redshi9ft initiated.....', '\n')
    #SQL Alchemy bulk insert directly to the staging table
    rearc_covid_19_world_test_proc.to_sql('covid19_data', conn, index=False, if_exists='append',chunksize= 1000, method='multi') 
    print('Moved to staging table', '\n')


# Converting json to pandas dataframe
def conv_dataframe(files):
    eng_jhu, rearc_covid_19_test, rearc_covid_19_world_test= ([] for i in range(3)) 

    for inp_f in files:
    
        if 'enigma-jhu' in inp_f:
            data = read_json_to_df(inp_f)
            eng_jhu.append(data)
               
        elif 'rearc-covid-19-testing-data' in inp_f:
            data = read_json_to_df(inp_f)
            rearc_covid_19_test.append(data)

        elif 'rearc-covid-19-world-cases-deaths-testing' in inp_f:
            data = read_json_to_df(inp_f)
            rearc_covid_19_world_test.append(data)

    eng_jhu = pd.concat(eng_jhu, axis=0, ignore_index=True)
    #rearc_covid_19_nyt_usa = pd.concat(rearc_covid_19_nyt_usa, axis=0, ignore_index=True)
    #rearc_covid_19_pred_mod = pd.concat(rearc_covid_19_pred_mod, axis=0, ignore_index=True)
    rearc_covid_19_test = pd.concat(rearc_covid_19_test, axis=0, ignore_index=True)
    rearc_covid_19_world_test= pd.concat(rearc_covid_19_world_test, axis=0, ignore_index=True)

    return eng_jhu, rearc_covid_19_test, rearc_covid_19_world_test


def read_json_to_df(input):
    with open(input, encoding="utf8") as f:
        data = [json.loads(line) for line in f]
        data = pd.DataFrame(data)
    return data
                
    
def process_staging_tables(conn, func):
    load_DWH_Params()
    s3 = s3_client()    
    buck_name =  'covid19-lake' 
    dire_path = [
    'enigma-jhu/json', 
    'rearc-covid-19-testing-data/json', 
    'rearc-covid-19-world-cases-deaths-testing/json', 
    ]       
    func(conn, s3, buck_name, dire_path)

    print(' All files processed')
      
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = create_engine('postgresql://{}:{}@{}:{}/{}'.format(*config['CLUSTER_SQL'].values()))

    process_staging_tables(conn, func=aws_load_staging_table)
 


if __name__ == "__main__":
    main()