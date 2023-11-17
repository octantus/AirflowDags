import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def top_10_domzone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    top_data_df['the_longest_dom'] = top_data_df.domain.str.split('.').str.get(-1)
    top_10_domzone = top_data_df.groupby('the_longest_dom', as_index = False) \
                                    .agg({'domain' : 'count'}) \
                                    .sort_values('domain', ascending = False) \
                                    .head(10)
    with open('top_10_domzone.csv', 'w') as f:
        f.write(top_10_domzone.to_csv(index = False, header=False))

        
def max_dom_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    top_data_df['domain_name_length'] = top_data_df.domain.str.split('.').str.get(0).apply(lambda x: len(x))
    max_dom_name = top_data_df.sort_values(['domain_name_length', 'domain'], ascending =(False, True)).head(1)        
    with open('max_dom_name.csv', 'w') as f:
        f.write(max_dom_name.to_csv(index = False, header = False))

        
def whereis_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    whereis_airflow = top_data_df.query('domain == "airflow.com"').iat[0,0]
    with open('whereis_airflow.csv', 'w') as f:
        f.write(str(whereis_airflow))
        
# def get_stat():
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))


# def get_stat_com():
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10_com.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_domzone.csv', 'r') as f:
        top10_z = f.read()
    with open('max_dom_name.csv', 'r') as f:
        max_name = f.read()
    with open('whereis_airflow.csv', 'r') as f:
        airflow_place = f.read()
    date = ds

    print(f'Top 10 domains of all zones for date {date}')
    print(top10_z)

    print(f'The longest domain name for date {date}')
    print(max_name)
    
    print(f'Placement for airflow.com for date {date}')
    print(airflow_place)


default_args = {
    'owner': 'a-kuksova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 11, 8),
}
schedule_interval = '0 12 * * *'

dag = DAG('domains_stat', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domzone',
                    python_callable=top_10_domzone,
                    dag=dag)

t3 = PythonOperator(task_id='max_dom_name',
                        python_callable=max_dom_name,
                        dag=dag)

t4 = PythonOperator(task_id='whereis_airflow',
                    python_callable=whereis_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
