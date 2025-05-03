from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from main import main

def process_pipeline():
    main()

dag = DAG(
    'nashville_data',
    description = 'DAG with PythonOperator',
    schedule = '*/5 * * * *',
    start_date = datetime(2023, 1, 1),
    catchup = False
)

process_task = PythonOperator(
    task_id = 'process_task',
    python_callable = process_pipeline,
    dag = dag
)