

from datetime import datetime, timedelta
from db_util import db_connect
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def tester():
    print('hello world')
    return


def readSnowflake():

    my_db = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_staging')
    sql = '''
                   select *
                   from ods.curr_items
                   limit 199
                   '''
    my_db.db_execute(sql=sql)
    rows = my_db.db_getAllRows()
    for row in rows:
        print(row)

      
with DAG("snowflaketest", start_date = datetime(2024,4,29), schedule_interval = "@daily", catchup=False) as dag:
    datarun = PythonOperator(
        task_id = "fullrun",
        python_callable = tester,
        dag=dag)    
    
    read = PythonOperator(dag=dag,
                          task_id="readSnowflake",
                          python_callable=readSnowflake)
    
