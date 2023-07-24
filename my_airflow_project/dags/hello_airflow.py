# my_airflow_project/dags/hello_airflow.py
# pylance-ignore: reportMissingImports

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_airflow',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Modify this as per your requirements
)

def print_hello():
    print("Hello, Airflow!")

with dag:
    t1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )
