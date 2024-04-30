from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def test():
    print('Hello World')
    return


with DAG("baseball", start_date = datetime(2024,4,29), schedule_interval = "@daily", catchup=False) as dag:
    datarun = PythonOperator(
        task_id = "fullrun",
        python_callable = test,
        dag=dag)    
    
