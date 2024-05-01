from datetime import datetime, timedelta
from baseball import updatestats
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


      
with DAG("baseball", start_date = datetime(2024,4,29), schedule_interval = "@daily", catchup=False) as dag:
    datarun = PythonOperator(
        task_id = "updatesheet",
        python_callable = updatstats,
        dag=dag)    
    
