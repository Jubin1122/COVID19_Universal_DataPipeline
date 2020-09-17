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
from data_cleaning import git_data_clean as gc
from sqlalchemy import create_engine

# Fetch json data from github API , clean and transform data, and load the data into staging table
def git_load_staging_tables(conn, file): 
    print('Data Cleaning initited.....', '\n')
    git_data  = gc(file)
    
    print('Loading to redshi9ft initiated.....', '\n')
    #SQL Alchemy bulk insert directly to the staging table
    git_data.to_sql('covid19_data', conn, index=False, if_exists='append',chunksize= 100, method='multi') 

# Resolve URL request to get response 200
def process_staging_tables(conn, url, func):

    try:
        urllib.request.urlopen(url).geturl()
    except urllib.error.HTTPError as e:
        if e.code == 429:
            time.sleep(5)
            resolve_redirects(url)
        raise
    response = requests.get(url)
    response_json = response.json()

    func(conn, response_json) 
    #conn.commit() 

    print('files processed')

        
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = create_engine('postgresql://{}:{}@{}:{}/{}'.format(*config['CLUSTER_SQL'].values()))
    print(conn)
    
    process_staging_tables(conn, url ='https://api.covid19api.com/summary', func=git_load_staging_tables)

    


if __name__ == "__main__":
    main()