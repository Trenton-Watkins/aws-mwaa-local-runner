from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, date,timedelta

import psycopg2
from db_util import db_connect
#
# current_date = date.today()
#
# current_month =date.today().month
#
# current_year = date.today().year
#
# print(current_date)
#
# print(current_year-1)
#
# print(f"{current_month}")


DAG_START_DATE = datetime(2021, 5, 14)

default_args = {
    'owner': 'Trenton Watkins',
    'depends_on_past': False,
    'start_date': DAG_START_DATE,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}


current_date = date(2023,5,30)

print(current_date.month)

connODS = {'target_conn_type': 'snowflake',
                           'target_conn_id': 'snowflake_ods_stg'
                           }

def runFunction(function, params):

    target_conn = db_connect(dag=DAG, conn_type=params['target_conn_type'], conn_id=params['target_conn_id'])
    target_conn.runFunctions(sql_dict=function)
    ret_var = True
    return ret_var


try:
    if current_date.month == 3:
        quarter = f'Q1 {current_date.year} '
        q = 'Q1'
        print(f"{quarter}")
    elif  current_date.month  == 6:
        quarter = f'Q2 {current_date.year} '
        q='Q2'
        print(f"{quarter}")
    elif current_date.month  == 9:
        quarter = f'Q3 {current_date.year} '
        q='Q3'
        print(f"{quarter}")
    elif current_date.month  == 12:
        quarter = f'Q4 {current_date.year} '
        q='Q4'
        print(f"{quarter}")
    else:
        quarter = 'NULL'
        q='q1'
        print("Not quarter end")
except:
    pass



sql =  {"1 - run to pull vbi" : f"""INSERT INTO ods."TRANSACTIONS" (TRAN_TYPE,TRAN_SUB_TYPE,TRAN_DATE,TRAN_QTY,CREATED_AT,SALE_DATE,TRAN_SUB_TYPE_ID,TRAN_GL_DATE,TRAN_AMT,REBATE_TOTAL,REBATE_PERCENTAGE ,REBATE_BILLING_METHOD,BRAND_MARKETING_ID)
        SELECT '300','Volume Based Incentive',CURRENT_TIMESTAMP(),1,CURRENT_TIMESTAMP(),CURRENT_DATE(),'128',CURRENT_DATE(),rebateamt,rebateamt,rebatepercentage, billing_method_id,VENDOR_FUNDING__VOLUME_BASE_ID  FROM (
        WITH receipts AS (SELECT VENDOR_ID ,VENDOR_NAME ,RECEIPT_QUARTER ,sum(RECEIPT_AMOUNT)AS amount FROM ods.VBI_NS_RECEIPTS 
        WHERE RECEIPT_QUARTER = '{quarter}'
        GROUP BY VENDOR_ID ,VENDOR_NAME ,RECEIPT_QUARTER )
        SELECT *,
        COALESCE (CASE
            WHEN (AMOUNT/ {q}_CONTRACT_BASELINE) -1 >= (Q4_GROWTH_THRESHOLD/100) THEN (Q4_REBATE/100) * AMOUNT
            WHEN (AMOUNT/ {q}_CONTRACT_BASELINE) -1 >= (Q3_GROWTH_THRESHOLD/100) THEN (Q3_REBATE/100) * AMOUNT
            WHEN (AMOUNT/ {q}_CONTRACT_BASELINE) -1 >= (Q2_GROWTH_THRESHOLD/100) THEN (Q2_REBATE/100) * AMOUNT
            WHEN (AMOUNT/ {q}_CONTRACT_BASELINE) -1 >= (Q1_GROWTH_THRESHOLD/100) THEN (Q1_REBATE/100) * AMOUNT
        END,0) AS rebateamt,
        COALESCE (CASE
            WHEN (AMOUNT/ {q}_CONTRACT_BASELINE) -1 >= (Q4_GROWTH_THRESHOLD/100) THEN (Q4_REBATE/100)
            WHEN (AMOUNT/ {q}_CONTRACT_BASELINE) -1 >= (Q3_GROWTH_THRESHOLD/100) THEN (Q3_REBATE/100) 
            WHEN (AMOUNT/ {q}_CONTRACT_BASELINE) -1 >= (Q2_GROWTH_THRESHOLD/100) THEN (Q2_REBATE/100) 
            WHEN (AMOUNT/ {q}_CONTRACT_BASELINE) -1 >= (Q1_GROWTH_THRESHOLD/100) THEN (Q1_REBATE/100) 
        END,0) AS rebatepercentage
        FROM receipts rr
        INNER JOIN (SELECT * FROM  ods.VOLUME_BASED_INCENTIVE  WHERE approval_status_id =2  AND is_inactive = 'F') vbi ON vbi.vendor_id = rr.vendor_id
        LEFT JOIN ods.BRAND_RECORDS br ON br.BRAND_RECORDS_ID = vbi.brand_id
        WHERE '{current_date}' >= START_DATE and '{current_date}' <= END_DATE)
        WHERE rebateamt >0
        """}

print(sql)

with DAG('ods_vbi', default_args=default_args,
          schedule_interval=None, catchup=False, max_active_runs=1) as dag:


    function = sql
    params = connODS
    task_id = "volume_based_incentives"

    VBITrans = PythonOperator(dag=dag,
                                    task_id=task_id,
                                    python_callable=runFunction,
                                    op_kwargs={'function': function, 'params': params})