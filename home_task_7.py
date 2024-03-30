"""
Зарегистрируйтесь в ОрепWeatherApi (https://openweathermap.org/api)
— Создайте ETL, который получает температуру в заданной вами локации, и
дальше делает ветвление:

• В случае, если температура больше 15 градусов цельсия — идёт на ветку, в которой есть оператор,
выводящий на экран «тепло»;
• В случае, если температура ниже 15 градусов, идёт на ветку с оператором, который выводит в
консоль «холодно».
Оператор ветвления должен выводить в консоль полученную от АРI температуру.
— Приложите скриншот графа и логов работы оператора ветвленния.

"""
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {'owner': 'airflow', 'retry_delay': timedelta(minutes=5),
                 # 'start_date': airflow.utils.dates.days_ago(2), # 'end_date': datetime(),
                 # 'depends_on_past': False, # 'email': ['airflow@example.com'],
                 # 'email_on_failure': False, #'email_on_retry': False, # If a task fails,
                 # retry it once after waiting # at least 5 minutes #'retries': 1,
                 }


def start_task():
    print('start tasks')


def get_weather():

    answer = requests.get('http://api.openweathermap.org/data/2.5/weather?q=Batumi, '
                          'ge&APPID=')
    return round(float(answer.json()['main']['temp']) - 273.15, 2)


def chose(x):
    print(x)
    if x >= 15:
        return 'warm_task'
    return 'cold_task'


def warm():
    print('тепло')


def cold():
    print('холодно')


dag_get = DAG(dag_id='get_weather', default_args=default_args,
              schedule_interval='@once',
              dagrun_timeout=timedelta(minutes=60),
              description='use api in airflow',
              start_date=days_ago(1))

start_task = PythonOperator(task_id='execute_task', python_callable=start_task, dag=dag_get)
get_weather_task = PythonOperator(task_id='get_weather_task', python_callable=get_weather, dag=dag_get)
chose_task = BranchPythonOperator(task_id='chose_task', python_callable=chose, dag=dag_get,
                                  op_args=[get_weather_task.output])
warm_task = PythonOperator(task_id='warm_task', python_callable=warm, dag=dag_get)
cold_task = PythonOperator(task_id='cold_task', python_callable=cold, dag=dag_get)

start_task >> get_weather_task >> chose_task >> [warm_task, cold_task]

