from datetime import datetime, timedelta
from newwnba import run_wnba
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


with DAG("wnbaupdate", start_date = datetime(2024,4,29), 
schedule_interval = '*/15 15-23 * * *', catchup=False) as dag:
    datarun = PythonOperator(
        task_id = "wnbaupdate",
        python_callable = run_wnba,
        dag=dag)    