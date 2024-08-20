from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from db_util import db_connect
import genGlAllocationsSql as glSql

ALERT_MESSAGE = "Daily load seems to have failed. WARNING: Updates for this run will be delayed."
ENV = "PRODUCTION" if Variable.get("AIRFLOW_ENV", default_var="stg") == "prd" else "STAGING"

DAG_START_DATE = datetime(2021, 5, 14)

default_args = {
    'owner': 'Robert Adams',
    'depends_on_past': False,
    'start_date': DAG_START_DATE,
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}


def truncateWorkTables():

    a = writeConn.db_execute('truncate ods.gl_allocation_header')
    a = writeConn.db_execute('truncate ods.gl_order_allocation_header')
    a = writeConn.db_execute('truncate ods.gl_allocation_detail')
    a = writeConn.db_execute('truncate ods.gl_order_allocation_detail')

    return
def getPeriod():
    sql = glSql.getPeriod
    readConn.db_execute(sql)
    row = readConn.db_getOneRow()
    cn = readConn.columnName(row)
    period = cn.calendaryearmonth
    periodStartDate = cn.month_start_date
    periodEndDate = cn.month_end_date

    return period, periodStartDate, periodEndDate

def getRules():
    sql = glSql.getRules
    a = readConn.db_execute(sql)

    rows = readConn.db_getAllRows()

    return rows

def getAccounts():
    sql = glSql.getAccountsToAlloc
    a = readConn.db_execute(sql)

    rows = readConn.db_getAllRows()

    return rows


def processGlAllocRules(rowCn, period, periodStartDate, periodEndDate):

    status = None
    ruleId = rowCn.id
    accounts = rowCn.use_accounts

    if rowCn.rule == 'all':
        location = '0'
        category = "'all'"
        if not rowCn.use_order:
            groupBy = 'period_end_date'
        else:
            groupBy = 'period_end_date, dje.ORDER_ID'
    elif rowCn.rule == 'location':
        location = 'dje.LOCATION_ID'
        category = "'all'"
        if not rowCn.use_order:
            groupBy = 'period_end_date, dje.LOCATION_ID'
        else:
            groupBy = 'period_end_date, dje.ORDER_ID, dje.LOCATION_ID'
    elif rowCn.rule == 'category':
        location = '0'
        category = 'dje.GL_PRODUCT_CATEGORY'
        if not rowCn.use_order:
            groupBy = 'period_end_date, dje.GL_PRODUCT_CATEGORY'
        else:
            groupBy = 'period_end_date, dje.ORDER_ID, dje.GL_PRODUCT_CATEGORY'
    elif rowCn.rule == 'location/category':
        location = 'dje.LOCATION_ID'
        category = 'dje.GL_PRODUCT_CATEGORY'
        if not rowCn.use_order:
            groupBy = 'period_end_date, dje.LOCATION_ID, dje.GL_PRODUCT_CATEGORY'
        else:
            groupBy = 'period_end_date, dje.ORDER_ID, dje.LOCATION_ID, dje.GL_PRODUCT_CATEGORY'

    if not rowCn.use_order:
        sql = glSql.insertAllocHeader
    else:
        sql = glSql.insertOrderAllocHeader
    sql = sql.format(runId='99', ruleId=ruleId, location=location, multiplier=rowCn.multiplier,
                     startDate=periodStartDate, endDate=periodEndDate,
                     category=category, accounts=accounts, period=period, groupBy=groupBy)
    print(sql)
    writeConn.db_execute(sql, commit=True)

    if not rowCn.use_order:
        sql = glSql.insertAllocDetail
    else:
        sql = glSql.insertOrderAllocDetail
    sql = sql.format(ruleId=ruleId, accounts=accounts, multiplier=rowCn.multiplier, startDate=periodStartDate,
                     endDate=periodEndDate, )
    print(sql)
    writeConn.db_execute(sql, commit=True)
    writeConn.db_commit()

    return status

def processGlAllocations(rowCn, period, periodStartDate, periodEndDate):
    allocId = rowCn.id
    ruleId = rowCn.rule_id
    accounts = rowCn.use_accounts
    multiplier = rowCn.multiplier

    sql = glSql.deleteGlAlloc.format(allocId=allocId, endDate=periodEndDate)
    print(sql)
    writeConn.db_execute(sql)

    sql = glSql.deleteGlOrderAlloc.format(allocId=allocId, endDate=periodEndDate)
    print(sql)
    writeConn.db_execute(sql)

    if not rowCn.use_order:
        sql = glSql.createGlAlloc
    else:
        sql = glSql.createGlOrderAlloc
    sql = sql.format(ruleId=ruleId, accounts=accounts, startDate=periodStartDate, endDate=periodEndDate,
                     multiplier=multiplier)
    print(sql)
    writeConn.db_execute(sql)

    if not rowCn.use_order:
        sql = glSql.createGlAllocOob
    else:
        sql = glSql.createGlAllocOrderOob

    print(sql)
    writeConn.db_execute(sql)

    if not rowCn.use_order:
        sql = glSql.updateGlAlloc
    else:
        sql = glSql.updateGlOrderAlloc
    print(sql)
    writeConn.db_execute(sql)

    if not rowCn.use_order:
        sql = glSql.insertToGlAllocation
    else:
        sql = glSql.insertToGlOrderAllocation
    sql = sql.format(allocId=allocId, ruleId=ruleId, endDate=periodEndDate)
    print(sql)
    writeConn.db_execute(sql, commit=True)

    if rowCn.use_order:
        sql = glSql.insertOrderToGlAllocation.format(allocId=allocId, endDate=periodEndDate)
        writeConn.db_execute(sql, commit=True)

    writeConn.db_execute('drop table IF EXISTS gl_alloc', commit=True)
    writeConn.db_execute('drop table IF EXISTS gl_order_alloc', commit=True)
    writeConn.db_execute('drop table IF EXISTS oob', commit=True)
    writeConn.db_commit()

def processRules():
    global readConn, writeConn
    readConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_staging')
    writeConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_staging')

    truncateWorkTables()
    period, periodStartDate, periodEndDate = getPeriod()
    rows = getRules()


    for row in rows:

        rowCn = readConn.columnName(row)
        status = processGlAllocRules(rowCn, period, periodStartDate, periodEndDate)

def processAllocations():
    global readConn, writeConn
    readConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_staging')
    writeConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_staging')

    period, periodStartDate, periodEndDate = getPeriod()
    rows = getAccounts()

    for row in rows:
        rowCn = readConn.columnName(row)
        status = processGlAllocations(rowCn, period, periodStartDate, periodEndDate)



with DAG('ods_gl_allocations', default_args=default_args,
          schedule_interval=None, catchup=False, max_active_runs=1) as dag:


    spProcessRules = PythonOperator(dag=dag,
                                    task_id='process_allocation_rules',
                                    python_callable=processRules,
                                    provide_context=True,
                                    op_kwargs={})

    spProcessAllocation = PythonOperator(dag=dag,
                                    task_id='process_allocations',
                                    python_callable=processAllocations,
                                    provide_context=True,
                                    op_kwargs={})

spProcessRules >> spProcessAllocation