"""

1. Скачайте файлы boking.csv, client.csv и hotel.csv;
2. Создайте новый dag;
3. Создайте три оператора для получения данных и загрузите файлы.
Передайте дата фреймы в оператор трансформации;
4. Создайте оператор который будет трансформировать данные:
— Объедините все таблицы в одну;
— Приведите даты к одному виду;
— Удалите невалидные колонки;
— Приведите все валюты к одной;
5. Создайте оператор загрузки в базу данных;
6. Запустите dag.

"""

import pandas as pd
import numpy
import io
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task


@task
def csv_to_df_1():
    url = ('https://gbcdn.mrgcdn.ru/uploads/asset/5551670/attachment/'
           '6257a083503973164c0bb0571d41d9e8.csv')
    data = pd.read_csv(url)
    return data


@task
def csv_to_df_2():
    url = ('https://gbcdn.mrgcdn.ru/uploads/asset/5551674/attachment/'
           '7c6bf202bd10996ca60a2593f755d4f4.csv')
    data = pd.read_csv(url)
    return data


@task
def csv_to_df_3():
    url = ('https://gbcdn.mrgcdn.ru/uploads/asset/5551688/attachment/'
           '3ed446d2c750d05b6c177f62641af670.csv')
    data = pd.read_csv(url)
    return data


@task
def transform(data_1, data_2, data_3):
    booking = data_1
    client = data_2
    hotel = data_3
    common = booking.merge(client, on='client_id')
    new_common = common.merge(hotel, on='hotel_id')
    new_common['booking_date'] = pd.to_datetime(new_common['booking_date'], format='mixed')
    new_common.drop(['client_id', 'hotel_id'], axis=1, inplace=True)
    new_common['booking_cost'] = numpy.where(new_common.currency == 'EUR', new_common.booking_cost * 0.86,
                                             new_common.booking_cost)
    new_common.replace({'EUR': 'GPB'}, inplace=True)
    mean_cost = new_common['booking_cost'].mean().round()
    new_common['booking_cost'] = new_common['booking_cost'].fillna(mean_cost)
    mean_age = new_common['age'].mean().round()
    new_common['age'] = new_common['age'].fillna(mean_age)
    new_common['currency'] = new_common['currency'].fillna('GPB')
    return new_common


@task()
def load(transformed_data):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # Convert the DataFrame to a CSV file-like object
    csv_buffer = io.StringIO()
    transformed_data.to_csv(csv_buffer, index=False, header=False)

    # Use the COPY command to load the CSV file into the table
    csv_buffer.seek(0)
    cur.copy_from(csv_buffer, 'temp_table', sep=",")
    conn.commit()

    # Close the cursor and connection
    cur.close()


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

with DAG('pandas_etl', schedule_interval=None) as dag:
    data1 = csv_to_df_1()
    data2 = csv_to_df_2()
    data3 = csv_to_df_3()
    create_employees_table = PostgresOperator(
        task_id="create_temp_table",
        postgres_conn_id='postgres_default',
        sql="CREATE TABLE IF NOT EXISTS temp_table ("
            "booking_date DATE,"
            "room_type TEXT, "
            "booking_cost NUMERIC,"
            "currency TEXT,"
            "age REAL,"
            "name_x TEXT,"
            "type TEXT,"
            "name_y TEXT,"
            "address TEXT);", )
    data = transform(data1, data2, data3)
    load(data)
