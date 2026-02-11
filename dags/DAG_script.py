import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from elasticsearch import Elasticsearch
from elasticsearch import helpers

import psycopg2 as db

import pandas as pd
import numpy as np

# Function for extracting data from postgre
def fetch_postgre():
    conn = db.connect(
        database="airflow",
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432')
    
    data = pd.read_sql('SELECT * FROM data', con = conn)
    
    # save as csv
    data.to_csv('/opt/airflow/dags/raw_data.csv', index=False)

# Cleaning function
def clean_data():
    df = pd.read_csv('/opt/airflow/dags/raw_data.csv', sep=',')
    df = df.dropna()
    df = df.drop_duplicates()
    df['Dt_Customer'] = pd.to_datetime(df['Dt_Customer'])             # Correct datatype
    df['Education'] = df['Education'].replace({'Graduation':'Undergraduate','2n Cycle':'Master','Basic':'High School'}) # Minimize cardinality
    df['Marital_Status'] = df['Marital_Status'].replace({'Divorced': 'Widow','Alone': 'Single',})                       # Minimize cardinality
    df.drop(df[df['Marital_Status'].isin(['Absurd', 'YOLO'])].index, inplace=True)                                      # Drop outlier
    df.columns = (df.columns
                .str.replace(r'([a-z])([A-Z])', r'\1_\2', regex=True)   # Add underscore between words
                .str.replace(r'(\d+)', r'_\1', regex=True)              # Add underscore between number
                .str.replace(' ', '_')                                  # Transform whitespace to underscore
                .str.replace(r'_+', '_', regex=True)                    # Delete double underscore (if exist)
                .str.lower())                                           # Transform case
    df['age']=2022-df['year_birth']                                     # Create new column

    df['dt_customer'] = pd.to_datetime(df['dt_customer'])
    today = pd.Timestamp('2022-01-01')
    df['customer_years'] = (today - df['dt_customer']).dt.days / 365.25
    df['customer_years'] = df['customer_years'].round(2)                # Create new column

    df= df.drop(columns=['response', 'z_cost_contact', 'z_revenue', 'year_birth']) # Drop unused columns

    df['num_no_deal_purchases']=df['num_web_purchases']+df['num_catalog_purchases']+df['num_store_purchases']-df['num_deals_purchases'] # Create new column

    df = df[df['num_no_deal_purchases'] >= 0] # Drop outlier

    del_income_index = df['income'].idxmax()
    df = df.drop(index=del_income_index) # Drop outlier

    del_age_index = df.nlargest(3, 'age').index
    df = df.drop(index=del_age_index) # Drop outlier
    
    df.to_csv('/opt/airflow/dags/clean_data.csv', sep=',', index=False)

# Upload to elastic search
def load_elastic():
    # Connect with Elastic Search
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/clean_data.csv')

    # Transform to dictionary
    records = df.to_dict(orient="records")

    # actions untuk bulk upload
    actions = []
    for doc in records:
        action = {
            "_index": "customer_analysis",   
            "_source": doc       
        }
        actions.append(action)

    # bulk insert ke elastic
    response = helpers.bulk(es, actions)
    print(response)

default_args = {
    'owner': 'rifqi',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('Project',
         default_args=default_args,
         schedule_interval='10,20,30 9 * 11 6',      # '0 * * * *',
         ) as dag:
    
    fetchPostgre = PythonOperator(task_id='fetch_postgre_data',
                                 python_callable=fetch_postgre)
    
    cleanData = PythonOperator(task_id='clean_data',
                                 python_callable=clean_data)
    
    uploadElastic = PythonOperator(task_id='upload_to_elastic',
                                 python_callable=load_elastic)

fetchPostgre >> cleanData >> uploadElastic 