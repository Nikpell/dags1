"""
Задание 1: Зарегистрируйте аккаунты для использования Yandex weather API -
 https://developer.tech.yandex.ru/services и Open weather API -
 https://home.openweathermap.org/users/sign_up используя следующие ссылки.
Создать телеграмм бота (инструкция https://telegram.org/faq#q-how-do-i-create-a-bot)
и прислать имя бота в чат.
Документация по Yandex Weather:
https://yandex.com/dev/weather/doc/en/
https://yandex.ru/pogoda/b2b/console
"""
import requests
from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(dag_id='telega_exampl', default_args=default_args,
          schedule_interval='@once',
          description='use telegram')


def telegram():
    with open('dags/Screens/id.txt', 'r') as telegram_id:
        return telegram_id.read()


def get_weather_openweathermap():
    with open('dags/Screens/pass.txt', 'r') as file:
        password = file.read()
        answer = requests.get('http://api.openweathermap.org/data/2.5/weather?q=Batumi, '
                              'ge&APPID=' + password)
        return round(float(answer.json()['main']['temp']) - 273.15, 2)


def get_weather_yandex():
    with open('dags/Screens/yandex.txt', 'r') as file:
        password = file.read()
        headers = {
            'X-Yandex-Weather-Key': password
        }

        response = requests.get('https://api.weather.yandex.ru/v2/forecast?lat=41.651102&lon=41.636267',
                                headers=headers)
        return response.json()['fact']['temp']


def text_to_telegram():
    return f'openweathermap: {get_weather_open.output}, yandex: {get_weather_yandex.output}'


get_weather_open = PythonOperator(
    task_id='get_weather_task_open',
    python_callable=get_weather_openweathermap,
    dag=dag)

get_weather_yandex = PythonOperator(
    task_id='get_weather_task_yandex',
    python_callable=get_weather_yandex,
    dag=dag)

send_message_telegram_task = TelegramOperator(
    task_id="send_message_telegram",
    chat_id=telegram(),
    telegram_conn_id='telega_def',
    text=text_to_telegram(),
    dag=dag,
)

[get_weather_open, get_weather_yandex] >> send_message_telegram_task
