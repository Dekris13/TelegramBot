import logging
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

import datetime
import pandas as pd
import psycopg2
import config


log = logging.getLogger(__name__)

#ДАГ на ежедневную загрузку задолженности из файла
@dag( 
    schedule_interval='30 12 * * 1-5',  # Задаем расписание выполнения дага - каждый рабочий день в 12:30.
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['Telebot'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def Start_DAG_from_file_to_DB():

    def Load_From_File_To_DB():
        # Обновляем данные по задолженности
        pg_conn = psycopg2.connect(config.conn)
        cur = pg_conn.cursor()
        date = pd.read_excel(config.path_to_debt, index_col=None)
        for index, row in date.iterrows():            
                val1 = row['CustomerID']
                val2 = row ['MainDebt']
                val3 = row ['PenaltyFee']
                sql = '''insert into stage.debt (customer_id, main_debt, penalty_fee)
                values ({}, {}, {})
                on conflict (customer_id)
                DO UPDATE set main_debt = {}, penalty_fee = {}'''.format(val1, val2, val3, val2, val3)
                cur.execute(sql)
                pg_conn.commit()

                # Вставляем новые CustomerID в таблицу по учету показаний
                sql1 = '''insert into stage.readings (customer_id)
                values ({})
                on conflict (customer_id)
                DO NOTHING
                '''.format(val1)
                cur.execute(sql1)
                pg_conn.commit()
        cur.close()
        pg_conn.close()

    @task()
    def Task_Load_From_File_To_DB():
        Load_From_File_To_DB()
        
    Task_Load_From_File_To_DB()



#ДАГ на ежемесячную выгрузку показаний в файл 25 числа каждого месяца 
@dag( 
    schedule_interval='0 0 25 * *',  # Задаем расписание выполнения дага - 25 числа каждого месяца
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['Telebot'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def Start_DAG_from_DB_to_file():

    def Load_from_DB_to_file():
        sql = '''select * from stage.readings order by customer_id'''
        data = pd.read_sql_query(sql, con=config.engine)
        data.to_excel(config.path_to_readings, index=False, header=True)

    @task()
    def Task_Load_from_DB_to_file():
        Load_from_DB_to_file()
        
    Task_Load_from_DB_to_file()




Start_DAG_load_from_file = Start_DAG_from_file_to_DB()

Start_DAG_from_DB= Start_DAG_from_DB_to_file()