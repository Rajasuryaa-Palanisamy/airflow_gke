from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 25),
    'retries': 1,
}

dag = DAG('sample_airflow_dag', default_args=default_args, schedule_interval=None)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

task1 = PythonOperator(
    task_id='task1',
    python_callable=hello_world,
    dag=dag,
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=hello_world,
    dag=dag,
)

start >> task1 >> task2 >> end
