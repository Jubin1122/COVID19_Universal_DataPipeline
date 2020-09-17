import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#covid_data_world = config.get('S3', 'covid_data_world')
#covid_date_usa = config.get('S3', 'covid_data_usa')

dwh_iam_role_arn = config.get("IAM_ROLE", "dwh_role_arn")

# Drop Tables
covid19_staging_drop = "DROP TABLE IF EXISTS covid19_data;"
covid19USA_staging_drop = "DROP TABLE IF EXISTS covid19_usa_data;"
covid_conf_cases_world_drop = "DROP TABLE IF EXISTS covid_conf_cases_world cascade;"
covid_ser_act_cases_world_drop = "DROP TABLE IF EXISTS covid_ser_act_cases_world cascade;"
covid_deaths_world_drop = "DROP TABLE IF EXISTS covid_deaths_world cascade;"
covid_recov_world_drop = "DROP TABLE IF EXISTS covid_recov_world cascade;"
covid_world_drop = "DROP TABLE IF EXISTS covid_world cascade;"
covid_conf_cases_usa_drop = "DROP TABLE IF EXISTS covid_conf_cases_usa;"
covid_deaths_usa_drop = "DROP TABLE IF EXISTS covid_death_usa;"
covid_hospital_usa_drop = "DROP TABLE IF EXISTS covid_hosp_usa;"
covid_usa_drop = "DROP TABLE IF EXISTS covid_usa;"




# Create_tables

covid19_staging_create = ("""
create table covid19_data
(
    country varchar(45),
    state varchar(45),
    conf_cases integer,
    new_conf_cases integer,
    total_conf integer,
    act_cases integer,
    critical_cases integer,
    new_deaths integer,
    total_deaths integer,
    new_recov integer,
    total_recov integer,
    date timestamp
)
""")

covid19usa_staging_create = ("""
create table covid19_usa_data(
    date timestamp,
    state varchar(64),
    conf_cases integer,
    conf_cases_incr integer,
    death integer,
    death_incr integer,
    hospitalized integer,
    hospital_incr integer
)
""")


covid_conf_cases_world_create = ("""
create table if not exists covid_conf_cases_world
(
    country varchar(45),
    conf_cases integer,
    new_conf_cases integer,
    total_conf integer,
    date_conf timestamp primary key
)
""")

covid_ser_act_cases_world_create = ("""
create table if not exists covid_ser_act_cases_world
(
    state varchar(45),
    country varchar(45),
    act_cases integer,
    critical_cases integer,
    date_ser timestamp primary key
)
""")

covid_deaths_world_create = ("""
create table if not exists covid_deaths_world
(
    total_deaths integer,
    new_deaths integer,
    date_deaths timestamp primary key
)
""")


covid_recov_world_create = ("""
create table if not exists covid_recov_world
(
    total_recov integer,
    date_recov timestamp primary key
)
""")

covid_world_create = ("""
create table if not exists covid_world
(
    serial_id integer identity(0,1) sortkey,
    date_conf timestamp references covid_conf_cases_world(date_conf),
    date_ser timestamp references covid_ser_act_cases_world(date_ser),
    date_deaths timestamp references covid_deaths_world(date_deaths),
    date_recov timestamp references covid_recov_world(date_recov)
)
""")

covid_conf_cases_USA_create = ("""
create table if not exists covid_conf_cases_usa
(
    date_conf timestamp primary key,
    state varchar(64),
    conf_cases integer,
    conf_cases_incr integer
)
""")

covid_deaths_USA_create = ("""
create table if not exists covid_death_usa
(
    date_death timestamp primary key,
    death integer,
    death_incr integer
)
""")

covid_hospital_USA_create = ("""
create table if not exists covid_hosp_usa
(
    date_hosp timestamp primary key,
    hospitalized integer,
    hospital_incr integer
)
""")

covid_USA_create = ("""
create table if not exists covid_usa
(
    serial_id integer identity(0,1) sortkey,
    date_conf timestamp references covid_conf_cases_usa(date_conf),
    date_death timestamp references covid_death_usa(date_death),
    date_hosp timestamp references covid_hosp_usa(date_hosp),
)
""")


# Insert Records
covid_confirmed_world = ("""
insert into covid_conf_cases_world
(country,
conf_cases,
new_conf_cases, total_conf, date_conf)
select country, conf_cases, new_conf_cases, total_conf, date_conf
from
covid19_data
""")

covid_serious_active = ("""
insert into covid_ser_act_cases_world
(state,
country,
act_cases,
critical_cases,
date_ser)
select state,country, act_cases, critical_cases, date_ser
from covid19_data
""")

covid_deaths_world = ("""
insert into covid_deaths_world
(total_deaths,
new_deaths,
date_deaths)
select total_deaths, new_deaths, date_deaths
from covid19_data
""")

covid_recov_world = ("""
insert into covid_recov_world
(total_recov, date_recov)
select total_recov, date_recov
from covid19_data
""")

covid_confirmed_USA =("""
insert into covid_conf_cases_usa
(date_conf , state, conf_cases, conf_cases_incr)
select date_conf, state,
conf_cases, conf_cases_ince
from covid19_usa_data
""")

covid_deaths_USA =("""
insert into covid_deaths_world
(   date_deaths,
    death ,
    death_incr)
select date_deaths,
death, death_ince
from covid19_usa_data
""")

covid_hospital_USA = ("""
insert into covid_hosp_usa
(
    date_hosp,
    hospitalized integer,
    hospital_incr
)
select date_hosp,
hospitalized, hospital_ince
from covid19_usa_data
""")

# Count of rows for each table
covid19_staging_count = ("""
SELECT reltuples::bigint AS estimate FROM pg_class where relname='covid19_data';
""")

covid19usa_staging_count = ("""
SELECT reltuples::bigint AS estimate FROM pg_class where relname='covid19_usa_data';
""")

covid_conf_cases_world_count = ("""
SELECT reltuples::bigint AS estimate FROM pg_class where relname='covid_conf_cases_world';
""")


covid_ser_act_cases_world_count = ("""
SELECT reltuples::bigint AS estimate FROM pg_class where relname='covid_ser_act_cases_world';
""")


covid_deaths_world_count = ("""
SELECT reltuples::bigint AS estimate FROM pg_class where relname='covid_deaths_world';
""")

covid_recov_world_count = ("""
SELECT reltuples::bigint AS estimate FROM pg_class where relname='covid_recov_world';
""")

covid_conf_cases_USA_count = ("""
SELECT reltuples::bigint AS estimate FROM pg_class where relname='covid_conf_cases_usa';
""")

covid_deaths_USA_count = ("""
SELECT reltuples::bigint AS estimate FROM pg_class where relname='covid_death_usa';
""")

covid_hospital_USA_count = ("""
SELECT reltuples::bigint AS estimate FROM pg_class where relname='covid_hosp_usa';
""")


# duplicate for each table

covid_conf_cases_world_dup = ("""
SELECT country, conf_cases,new_conf_cases, total_conf, date_conf, 
sum(count(*)) as duplicates FROM covid_conf_cases_world
GROUP BY country, conf_cases, new_conf_cases, total_conf, date_conf HAVING count(*)>1;
""")


covid_ser_act_cases_world_dup = ("""
SELECT state,
country,
act_cases,
critical_cases,
date_ser, sum(count(*)) as duplicates FROM covid_ser_act_cases_world 
GROUP BY state,country,act_cases,critical_cases,
date_ser HAVING count(*)>1;
""")


covid_deaths_world_dup = ("""
SELECT total_deaths,
new_deaths,
date_deaths, sum(count(*)) as duplicates FROM covid_deaths_world 
GROUP BY total_deaths,new_deaths,
date_deaths HAVING count(*)>1;
""")

covid_recov_world_dup = ("""
SELECT total_recov, date_recov, sum(count(*)) as duplicates FROM covid_recov_world 
GROUP BY total_recov, date_recov, HAVING count(*)>1;
""")

covid_conf_cases_USA_dup = ("""
SELECT date_conf , state, conf_cases, conf_cases_incr, sum(count(*)) as duplicates
FROM covid_conf_cases_usa GROUP BY date_conf , state, conf_cases,
 conf_cases_incr_name HAVING count(*)>1;
""")

covid_deaths_USA_dup = ("""
SELECT date_deaths,
    death ,
    death_incr, sum(count(*)) as duplicates FROM covid_deaths_world
    GROUP BY date_deaths,
    death ,
    death_incr HAVING count(*)>1;
""")

covid_hospital_USA_dup = ("""
SELECT date_hosp,
    hospitalized integer,
    hospital_incr, sum(count(*)) as duplicates FROM covid_hosp_usa 
    GROUP BY date_hosp,
    hospitalized integer,
    hospital_incr HAVING count(*)>1;
""")



# Query Lists

#Create Tables
create_table_queries = [covid19_staging_create, covid19usa_staging_create, covid_conf_cases_world_create, covid_ser_act_cases_world_create, covid_deaths_world_create,
covid_recov_world_create, covid_world_create, covid_conf_cases_USA_create, covid_deaths_USA_create,
covid_hospital_USA_create, covid_USA_create ]

# Drop Tables
drop_table_queries = [covid19_staging_drop, covid19USA_staging_drop, covid_conf_cases_world_drop, covid_deaths_world_drop, covid_deaths_world_drop, covid_recov_world_drop,
covid_world_drop, covid_conf_cases_usa_drop, covid_deaths_usa_drop, covid_hospital_usa_drop,
covid_usa_drop]

# Insert Queries
insert_table_order = ['covid_conf_cases_world', 'covid_ser_act_cases_world', 'covid_deaths_world',
'covid_recov_world', 'covid_conf_cases_usa', 'covid_deaths_world', 'covid_hosp_usa']

insert_table_queries = [covid_confirmed_world, covid_serious_active,
covid_deaths_world, covid_recov_world, covid_confirmed_USA, covid_deaths_USA,
covid_hospital_USA]

# Row count
count_table_order = ['covid19_data', 'covid19_usa_data', 'covid_conf_cases_world', 'covid_ser_act_cases_world', 'covid_deaths_world',
'covid_recov_world', 'covid_conf_cases_usa', 'covid_deaths_world', 'covid_hosp_usa']
count_table_queries = [covid19_staging_count, covid19usa_staging_count,
covid_conf_cases_world_count, covid_ser_act_cases_world_count,
covid_deaths_world_count, covid_recov_world_count, covid_conf_cases_USA_count,
covid_deaths_USA_count, covid_hospital_USA_count]

# Query to find Duplicate records

table_order = ['covid_conf_cases_world', 'covid_ser_act_cases_world', 'covid_deaths_world',
'covid_recov_world', 'covid_conf_cases_usa', 'covid_deaths_world', 'covid_hosp_usa']
duplcate_queries = [covid_conf_cases_world_dup, covid_ser_act_cases_world_dup,
covid_deaths_world_dup, covid_recov_world_dup, covid_conf_cases_USA_dup,
covid_deaths_USA_dup, covid_hospital_USA_dup]
