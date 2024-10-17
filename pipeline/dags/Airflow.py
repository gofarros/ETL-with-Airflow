'''
=================================================
Milestone 3

Nama  : Ghaffar Farros
Batch : FTDS-HCK-019

Program ini dibuat untuk melakukan automatisasi transform dan load data dengan airflow, hingga data masuk ke
elasticsearch untuk divisualisasikan dengan kibana. Adapun dataset yang dipakai adalah dataset mengenai 
inventaris mobil.
=================================================
'''

import datetime as dt
from datetime import timedelta
from sqlalchemy import create_engine 
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from requests.exceptions import ConnectionError
import pandas as pd

'''
Melakukan loading data dari direktori data di container kedalam postgresql sebagai database.
'''
def load_to_postgres():
    database = "airflow"    # define database name  
    username = "airflow"    # define username
    password = "airflow"    # define password
    host = "postgres"       # define host name

    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/data/P2M3_Ghaffar_Farros_data_raw.csv') #read csv
    df.to_sql('table_m3', conn, index=False, if_exists='replace')  #save ke sql, dan replace kalau sudah ada

'''
Berisi script untuk mengambil data dari PostgreSQL untuk digunakan pada container.
data selalu dikoneksikan dengan database server PostgreSQL untuk keamanan database.
'''
#fetch
def fetch_data():    
    database = "airflow"    # define database name
    username = "airflow"    # define username
    password = "airflow"    # define password
    host = "postgres"       # define host name
    
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
    
    engine = create_engine(postgres_url)
    conn = engine.connect()                 #connect dengan postgres_url

    df = pd.read_sql_query("select * from table_m3", conn) #querying dengan select all di table_m3
    df.to_csv('/opt/airflow/data/P2M3_Ghaffar_Farros_data_raw.csv', sep=',', index=False) #save ke csv
    
'''
berisi script untuk melakukan Data Cleaning dan penyimpanan ke CSV file. Data raw di read dari container
lalu dilakukan cleaning (duplicate, normalisasi kolom, handle missing value), kemudian
disave kembali dalam bentuk csv ke folder data di container sebagai data_clean
'''
#data cleaning
def cleaning():
    df=pd.read_csv('/opt/airflow/data/P2M3_Ghaffar_Farros_data_raw.csv')
    df.drop_duplicates(inplace=True)            #drop duplicate
    def clean_column_name(col_name):    
        col_name = col_name.lower()             # lowering nama kolom
        col_name = col_name.replace(' ', '_')   # spasi ganti _
        col_name = col_name.strip()             # delete jika ada kelebihan spasi
        return col_name
    df.columns = [clean_column_name(col) for col in df.columns]
    df.fillna(0, inplace=True)
    df.to_csv('/opt/airflow/data/P2M3_Ghaffar_Farros_data_clean.csv', index=False)

'''
berisi script untuk me-load CSV yang berisi data yang sudah clean dan memasukkannya ke Elasticsearch.
melakukan iterasi dan convert kedalam bentu dictionary agar dapat dibaca dan masuk ke elasticsearch
'''
# upload to elasticsearch
def upload_to_elasticsearch():
    try:
        es = Elasticsearch(hosts=["http://elasticsearch:9200"])
        
        # Load cleaned data with Pandas
        df = pd.read_csv('/opt/airflow/data/P2M3_Ghaffar_Farros_data_clean.csv')
        
        # Convert row menjadi dictionary
        for i, row in df.iterrows():            
            doc = row.to_dict()     
            try:
                res = es.index(index="table_m3", id=i+1, body=doc)
                print(f"Indexed document {i+1} in Elasticsearch: {res}")
            except Exception as e:
                print(f"Error indexing document {i+1}: {e}")
                continue  # Skip doc dan melanjutkan

        print(f'Completed inputting {len(df)} documents')

    except ConnectionError:
        print("Failed to connect to Elasticsearch")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

'''
Menentukan default argument owner dan setting tanggal dalam bentuk WIB dengan crontab.
Menetapkan python sebagai operator yang digunakan pada masing-masing fungsi, lalu menentukan
urutan sesuai pada DAG graph
'''
#default arguments
default_args = {
    'owner': 'gofar',
    'start_date': dt.datetime(2024, 9, 16, 11, 40, 0) - dt.timedelta(hours=7),
}

with DAG('M3_DAG',
         default_args=default_args,
         schedule_interval= '30 6 * * *' , # Penjadwalan dilakukan setiap jam 06:30 dengan cron
         ) as dag:

#dibawah ini menetapkan operator yang digunakan pada masing-masing fungsi yang akan digunakan
    load_csv = PythonOperator(
                                task_id='load_to_postgres',
                                python_callable=load_to_postgres,
                                dag=dag) 
    
    fetch_csv = PythonOperator(
                                task_id='fetch_data_postgres',
                                python_callable=fetch_data,
                                dag=dag) 
        
    clean_data = PythonOperator(task_id='clean',
                                 python_callable=cleaning,
                                dag=dag)

    upload_elastic = PythonOperator(
                                task_id='upload_to_elastic',
                                python_callable=upload_to_elasticsearch,
                                dag=dag)

# menentukan alur graph
load_csv >> fetch_csv >> clean_data >> upload_elastic