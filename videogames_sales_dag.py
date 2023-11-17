import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

year = 1994 + hash(f'a-kuksova') % 23
data_source = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a-kuksova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 11, 13)
}

@dag(default_args=default_args, schedule_interval='0 8 * * *', catchup=False)
def videogames_sales_akuksova():
    
    @task() # Подгружаем данные
    def get_data():
        vg_data = pd.read_csv(data_source)
        vg_data = vg_data[df.Year == year].reset_index()
        return vg_data

    @task() # Какая игра была самой продаваемой в этом году во всем мире?
    def get_game_of_the_year(vg_data):
        game_of_the_year = vg_data.groupby('Name', as_index=False) \
                               .agg({'Global_Sales':'sum'}) \
                               .sort_values('Global_Sales', ascending=False) \
                               .head(1)
        return game_of_the_year.to_string(index=False).strip()[20:]
    
     @task() # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def get_top_europe(vg_data):
    top_europe = vg_data.groupby('Genre', as_index=False) \
                    .agg({'EU_Sales':'sum'}) \
                    .sort_values('EU_Sales', ascending=False) \
                    .head(1)
    return top_europe.to_string(index=False).strip()[17:]

     @task() # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def get_platform_na(vg_data):
        platform_na = vg_data[vg_data.NA_Sales > 1].groupby(['Platform','Name'], as_index=False) \
                                                   .agg({'Name':'count'})
        platform_na = platform_na.groupby('Platform',as_index=False) \
                                 .agg({'Name':'sum'}) \
                                 .sort_values('Name', ascending=False) \
                                 .head(1)
        return platform_na.to_string(index=False).strip()[20:]

     @task() # У какого издателя самые высокие средние продажи в Японии?
    def get_topmean_japan(vg_data):
        topmean_jp_pub = vg_data.groupby('Publisher', as_index=False) \
                    .agg({'JP_Sales':'mean'}) \
                    .sort_values('JP_Sales', ascending=False) \
                    .head(1)
        return topmean_jp_pub.to_string(index=False).strip()[21:]

     @task() # Сколько игр продались лучше в Европе, чем в Японии?
    def get_diff_eu_jp(vg_data):
        diff_eu_jp = vg_data.groupby('Name', as_index=False) \
                            .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        diff_eu_jp = diff_eu_jp[diff_eu_jp.EU_Sales > diff_eu_jp.JP_Sales].Name.nunique()
        return diff_eu_jp

    @task() # Вывод в логи
    def print_data(game_of_the_year, top_europe, platform_na, topmean_jp_pub, diff_eu_jp):
        print(f'''Videogames data for {year} year.
                1) Game of the year: {game_of_the_year}
                2) Top EU sales by genre: {top_europe}
                3) Top NA sales by platform: {platform_na}
                4) Top JP mean sales by publisher: {topmean_jp_pub}
                5) Count of games that sold better in EU than JP: {diff_eu_jp}''')

    vg_data = get_data()
        game_of_the_year = get_game_of_the_year(vg_data)
        top_europe = get_top_europe(vg_data)
        platform_na = get_platform_na(vg_data)
        topmean_jp_pub = get_topmean_japan(vg_data)
        diff_eu_jp = get_diff_eu_jp(vg_data)
        print_data(game_of_the_year, top_europe, platform_na, topmean_jp_pub, diff_eu_jp)

videogames_sales_akuksova = videogames_sales_akuksova()