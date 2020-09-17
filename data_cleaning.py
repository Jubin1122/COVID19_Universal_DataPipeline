
import pandas as pd
import numpy as np
import json
from datetime import datetime

# DataSource from Postman API
def git_data_clean(file):
    for line in file:
        if line == "Countries":
            df = pd.DataFrame(file['Countries'])

            df['conf_cases'] = df['TotalConfirmed'] - df['NewConfirmed']
            df[['state', 'act_cases', 'critical_cases']] = pd.DataFrame([[None, np.nan, np.nan]], index=df.index)

            df = df.loc[: , ['Country', 'state', 'conf_cases', 'NewConfirmed',
            'TotalConfirmed', 'act_cases', 'critical_cases', 'NewDeaths',
            'TotalDeaths', 'NewRecovered', 'TotalRecovered', 'Date']]

            df['Date'] = pd.to_datetime(df['Date'])

            for columns in df.columns:
                if columns in ['Country', 'state']:
                    df[columns] = df[columns].astype(str)

                if columns in ['act_cases', 'critical_cases']:
                    df[columns] = df[columns].fillna(0).astype(np.int64)

            new_cols = ['country','state',
                'conf_cases',
                'new_conf_cases',
                'total_conf',
                'act_cases',
                'critical_cases',
                'new_deaths',
                'total_deaths',
                'new_recov',
                'total_recov',
                'date']
            df.rename(columns=dict(zip(df.columns[:], new_cols)),inplace=True)


    return df


# Data From AWS S3 JHU
def s3jhu_data_clean(df):

    df[['new_conf_cases', 'critical_cases', 'new_deaths', 'new_recov']] = pd.DataFrame([[np.nan, np.nan, np.nan, np.nan]], index=df.index)

    for columns in df.columns:
        if columns in ['new_conf_cases', 'critical_cases', 'new_deaths', 'new_recov']:
            df[columns] = df[columns].fillna(0).astype(np.int64)


    df['total_conf'] = df['new_conf_cases'] + df['confirmed']
    df['total_deaths'] = df['new_deaths'] + df['deaths']
    df['total_recov'] = df['new_recov'] + df['recovered']


    df = df.loc[:, ['country_region', 'province_state', 'confirmed', 'new_conf_cases', 'total_conf', 'active', 'critical_cases', 'new_deaths', 'total_deaths', 'new_recov', 'total_recov', 'last_update']]

    for col in df.columns:
        if df[col].dtypes == 'float64':

            df[col] = df[col].replace(np.nan, 0)
            df[col] = df[col].astype('int64')

    df['last_update'] = pd.to_datetime(df['last_update'])

    new_cols = ['country','state',
                'conf_cases',
                'new_conf_cases',
                'total_conf',
                'act_cases',
                'critical_cases',
                'new_deaths',
                'total_deaths',
                'new_recov',
                'total_recov',
                'date']
    df.rename(columns=dict(zip(df.columns[:], new_cols)),inplace=True)

    return df

# Data From AWS S3 USA data
def s3rearc_covid_19_test_clean(df):

    df = df.loc[:,['date', 'state', 'positive', 'positiveIncrease' , 'death', 'deathIncrease', 'hospitalized', 'hospitalizedIncrease']]

    df['date'] = pd.to_datetime(df['date'])
    var = ['positive', 'death', 'hospitalized', 'positiveIncrease', 'deathIncrease', 'hospitalizedIncrease']

    for col in var:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        df[col] = df[col].replace(np.nan, 0)

    new_cols = ['date',
    'state',
    'conf_cases',
    'conf_cases_incr',
    'death','death_incr',
    'hospitalized',
    'hospital_incr integer']
    df.rename(columns=dict(zip(df.columns[:], new_cols)),inplace=True)

    return df

# Data From AWS S3 world data
def s3rearc_covid_19_world_test_clean(df):

    df['conf_cases'] = df['total_cases'] - df['new_cases']

    df[['state', 'act_cases', 'critical_cases', 'new_recov', 'total_recov']] = pd.DataFrame([[None, np.nan, np.nan, np.nan, np.nan]], index=df.index)

    df = df.loc[:, ['location','state', 'conf_cases',
    'new_cases', 'total_cases', 'act_cases', 'critical_cases', 'new_deaths', 'total_deaths', 'new_recov', 'total_recov', 'date']]

    for col in df.columns:
        if df[col].dtypes == 'float64':
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            df[col] = df[col].replace(np.nan, 0)
        if col == 'date':
            df[col] = pd.to_datetime(df[col])
    new_cols = ['country','state',
                'conf_cases',
                'new_conf_cases',
                'total_conf',
                'act_cases',
                'critical_cases',
                'new_deaths',
                'total_deaths',
                'new_recov',
                'total_recov',
                'date']
    df.rename(columns=dict(zip(df.columns[:], new_cols)),inplace=True)



    return df

def kaggle_usa_clean(df):
    df[['conf_cases_incr', 'death_incr', 'hospitalized', 'hospitalized_inc']] = pd.DataFrame([[np.nan, np.nan, np.nan, np.nan]], index=df.index)

    df = df.loc[:,['date', 'state_name', 'confirmed', 'conf_cases_incr', 'deaths',
    'death_incr', 'hospitalized', 'hospitalized_inc']]

    for col in df.columns:
        if df[col].dtypes == 'float64':
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            df[col] = df[col].replace(np.nan, 0)
        if col == 'date':
            df[col] = pd.to_datetime(df[col])

    new_cols = ['date',
    'state',
    'conf_cases',
    'conf_cases_incr',
    'death',
    'death_incr',
    'hospitalized',
    'hospital_incr']
    df.rename(columns=dict(zip(df.columns[:], new_cols)),inplace=True)


    return df

def kaggle_world_clean(df):

    df[['state', 'new_recov']] = pd.DataFrame([[None, np.nan]], index=df.index)
    df['new_cases'] = pd.to_numeric(df['new_cases'], errors='coerce').astype('Int64')
    df['new_cases'] = df['new_cases'].replace(np.nan, 0)
    df['conf_cases'] = df['total_cases'] - df['new_cases']

    df['date'] = datetime(2019, 5, 22, 00, 27, 53)

    df = df.loc[:,['country', 'state', 'conf_cases', 'new_cases', 'total_cases', 'active_cases', 'serious_critical_cases', 'new_deaths', 'total_deaths', 'new_recov', 'total_recovered', 'date']]

    for col in df.columns:
        if df[col].dtypes == 'float64':
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            df[col] = df[col].replace(np.nan, 0)

        if col == 'new_deaths':
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            df[col] = df[col].replace(np.nan, 0)

    new_cols = ['country','state',
                'conf_cases',
                'new_conf_cases',
                'total_conf',
                'act_cases',
                'critical_cases',
                'new_deaths',
                'total_deaths',
                'new_recov',
                'total_recov',
                'date']
    df.rename(columns=dict(zip(df.columns[:], new_cols)),inplace=True)

    return df
