# Introduction
- The primary goal of the project is to define a universal data pipeline that will collect data from all scattered datasets found over various websites in different formats.

- Currently, various organizations are collecting data, but there is no unified platform that collects data from all sources and maintained a single repository on which researchers can work.

- As the final end-user would the researchers or biotech companies, thus this pipeline will collect data automatedly and update the repository at a specific time. Moreover, this pipeline would also perform data cleaning, and transformation, therefore, will help Machine learning developers and Web Developers to use it for various applications

# Dataset
#### Postman API - Source 1
`https://api.covid19api.com/summary`

During the present novel coronavirus (COVID-19) pandemic, those on the front lines—including health care professionals, researchers, and government experts—need quick, easy access to real-time critical data. This type of information exchange is what APIs do best, and as an API-first company, Postman is committed to providing whatever assistance we can in this area.

```
{
  "Global": {
    "NewConfirmed": 100282,
    "TotalConfirmed": 1162857,
    "NewDeaths": 5658,
    "TotalDeaths": 63263,
    "NewRecovered": 15405,
    "TotalRecovered": 230845
  },
  "Countries": [
    {
      "Country": "ALA Aland Islands",
      "CountryCode": "AX",
      "Slug": "ala-aland-islands",
      "NewConfirmed": 0,
      "TotalConfirmed": 0,
      "NewDeaths": 0,
      "TotalDeaths": 0,
      "NewRecovered": 0,
      "TotalRecovered": 0,
      "Date": "2020-04-05T06:37:00Z"
    },
```

This dataset will provide a summary of new and total cases per country updated daily.

#### AWS S3 Bucket; COVID19-lake
https://dj2taa9i652rf.cloudfront.net/

This is a data lake on AWS S3 which is regularly updated. Here, the data is in json format.

![Alt text](img/AWS_dataset.png?raw=true "Title")

### Kaggle
https://www.kaggle.com/roche-data-science-coalition/uncover

This dataset is composed of a curated collection of over 200 publicly available COVID-19 related datasets from sources like Johns Hopkins, the WHO, the World Bank, the New York Times, and many others. It includes data on a wide variety of potentially powerful statistics and indicators, like local and national infection rates, global social distancing policies, geospatial data on movement of people, and more.

Moreover, here the files are in CSV format.

# System Design
![Alt text](img/system_design.jpg?raw=true "Title")

### Redshift as a Postgres Database
Here, I have decided to use redshift cluster which will be used to load data into Postgres Analytical Tables.

All the AWS keys will parsed from the `dwh.cfg`.

Steps to create cluster, roles, security groups and database:
- First, we will run `create_cluster.py` , it will create the cluster with the specified IAM role.
- Then, to retrieve the Data warehouse endpoint and to give access to the ports we will run `cluster_endpoint.py`
- To view all the cluster details, we have to run the
`cluster_details.py`.
- Finally, `delete_cluster.py` will delete the cluster.

# ETL pipeline

- I have used three different python files handling the extract, transfer, and loading functionality from three separate data sources.
- First, we will run the `postman_etl.py`. It will extract, transform, and load the data from Postman API to the Staging Tables in Redshift.
- By running `AWS_s3_etl.py` , data will be extracted from the AWS s3 bucket COVID19-lake and pushed to the Staging Table after data cleaning and transformation.
- Finally, we will run the `kaggle_etl.py`. It will extract, transform, and load the data into the Staging Tables in Redshift.
- Also, `data_cleaning.py` will be called internally by all the above etl files. This will basically clean the data, remove null values , unwanted columns and performs other data transformation tasks.

- Moreover, I use ***sqlalchemy bulk insert*** technique, which will directly push data into AWS Redshift, thus saving S3 storage and other instances of costs.

# Database Design

- First of all we will run `create_tables.py`, it will drop the tables if previously exists and create the tables with respective relationships between them.

- Here, I have used a star schema data warehouse design. There are in total two star schema one for the **covid cases around the world** and one for the **covid cases in USA**.

<u> <b>Star Schema Data Warehouse design for covid cases around the world </b> </u>
![Alt text](img/World_db.png?raw=true "Figure 1")

<u> <b>Star Schema Data Warehouse design for covid cases in USA</b> </u>
![Alt text](img/covid_usa.png?raw=true "Figure 1")

### Quality checks
For the quality we will run `quality_check.py`.
- The first quality check will be performed by ***Quality_check()***. It will basically check whether the rows are successfully inserted or not
and if inserted, will reflect it on the console screen.

```
def Quality_check(cur, conn):
    """
    Whether insertion is successfull or not
    """
    i = 0
    for query in count_table_queries:
        print(" Analytical Table: {}..".format(count_table_order[i]))
        cur.execute(query)
        results = cur.fetchone()

        for res in results:
            if res <1:
                print("Data quality check failed. {} returned no results".format(count_table_order[i]))
            else:
                print("Rows successfully inserted:", results)
        i = i + 1

```

- The second quality check will be performed by ***check_duplicate_records()***. It will count() all the duplicate records across each column and then sum up all the counts. If the sum is greater than 1 it means duplicate records are present and vice versa.

```
def check_duplicate_records(cur, conn):
    """
    Check whether the table contain duplicate records or not
    """
    i = 0
    for query in duplcate_queries:
        print(" Analytical Table: {}..".format(table_order[i]))
        cur.execute(query)
        res = cur.fetchall()

        if res[0]['duplicates'] > 1:
            print("Duplicate records are present")
        else:
            print("No duplicate records found")
        i = i + 1
```

# Future Scenarios

- The ETL Pipeline is optimized to perform a bulk insert efficiently even if the data was increased by 100x. The type of architecture adopted for the ETL pipeline is well suited for the practical industrial scenario.

- It is highly stable, and automation can be done on Airflow, where various scheduling techniques can be chosen as per the requirements. The API's developed will not affect if the rows increases.

- Moreover, the chosen database, which is Postgres on Redshift, is highly efficient as it can process complex queries and various read operations on the database. It makes a copy of the data and distributes it across the nodes, thus makes it highly available.

- The database adopted here can be accessed by 100+ people, if they do not need to perform insert and update operations.
