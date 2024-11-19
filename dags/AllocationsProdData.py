from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from db_util import db_connect
import allocanrun as glSql

ALERT_MESSAGE = "Daily load seems to have failed. WARNING: Updates for this run will be delayed."
ENV = "PRODUCTION" if Variable.get("AIRFLOW_ENV", default_var="stg") == "prd" else "STAGING"

DAG_START_DATE = datetime(2024,9, 1)

default_args = {
    'owner': 'Trenton Watkins',
    'depends_on_past': False,
    'start_date': DAG_START_DATE,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}


# def truncateWorkTables():

#     a = writeConn.db_execute('truncate ods.gl_allocation_header')
#     a = writeConn.db_execute('truncate ods.gl_order_allocation_header')
#     a = writeConn.db_execute('truncate ods.gl_allocation_detail')
#     a = writeConn.db_execute('truncate ods.gl_order_allocation_detail')

#     return
def getPeriod():
    sql = glSql.getPeriod
    sql = sql.format(current_date = DAG_START_DATE)
    readConn.db_execute(sql)
    row = readConn.db_getOneRow()
    cn = readConn.columnName(row)
    period = cn.calendaryearmonth
    periodStartDate = cn.month_start_date
    periodEndDate = cn.month_end_date
    periodname = cn.quartertext

    return period, periodStartDate, periodEndDate,periodname

def getRules():
    sql = glSql.getRules
    a = readConn.db_execute(sql)

    rows = readConn.db_getAllRows()

    return rows

# def getAccounts():
#     sql = glSql.getAccountsToAlloc
#     a = readConn.db_execute(sql)

#     rows = readConn.db_getAllRows()

#     return rows

def processGlAllocRules(rowCn, period, periodStartDate, periodEndDate):
    status = None
    ruleId = rowCn.id
    tranType = rowCn.tran_type
    tranSubType = rowCn.tran_sub_type_id
    sqlAttribute = rowCn.rule_sql
    tranColumn = rowCn.tran_column
    rulename = rowCn.rule


    sql = getattr(glSql, sqlAttribute + 'Header')

    sql = sql.format(rule=rulename, ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,
                    tranType=tranType, tranSubType=tranSubType, period=period,  tranColumn=tranColumn)
    print(sql)
    writeConn.db_execute(sql, commit=True)


    sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format(rule = rulename, ruleId=ruleId, tranType=tranType, tranSubType=tranSubType, startDate=periodStartDate, endDate=periodEndDate,
                    tranColumn=tranColumn)
    print(sql)
    writeConn.db_execute(sql, commit=True)


    writeConn.db_commit()

    return status

def getRun():
    sql = glSql.getRun

    a = readConn.db_execute(sql)
    row = readConn.db_getOneRow()
    cn =readConn.columnName(row)
    runid = cn.run_id + 1
    return runid

def getMapODS():
    sql = glSql.getMapODS

    a = readConn.db_execute(sql)
    rows = readConn.db_getAllRows()
    return rows

def getMapNS():
    sql = glSql.getMapNS

    a = readConn.db_execute(sql)
    rows = readConn.db_getAllRows()
    return rows

def processGlAllocations(rowCn, periodStartDate, periodEndDate):
        
    ruleId = rowCn.alloc_rule_id
    mapId= rowCn.dje_map_id
    category = rowCn.category
    location = rowCn.location
    nsaccount =rowCn.accountnumber
    transubtypeid = rowCn.tran_sub_type_id 
    transubtype = rowCn.tran_sub_type
    trantype = rowCn.tran_type

    joinfilter =''
    joinclause =''
    if category is not None:
        joinfilter += f"AND dd.LEVEL2 =  mp.category  "
    
    if location is not None:
        joinfilter = f"AND dd.LEVEL2 = fcr.fc_id"
        joinclause += """    left join (
                            SELECT DISTINCT tr.order_id ,fc.NS_FC_ID as fc_id FROM ods."TRANSACTIONS" tr 
                    INNER JOIN ODS.NS_FC_XREF fc ON fc.NS_FC_ID =tr.LOCATION_ID 
                    WHERE tr.TRAN_SUB_TYPE_ID in (16,82) AND fc.FC_TYPE ='hj'
                    UNION all                    
                    SELECT DISTINCT tr.order_id ,fc.NS_FC_ID as fc_id FROM ods."TRANSACTIONS" tr 
                    INNER JOIN ODS.NS_FC_XREF fc ON fc.NS_FC_ID =tr.LOCATION_ID 
                    WHERE tr.TRAN_SUB_TYPE_ID in (16,82) AND fc_type ='veracore' AND  tr.order_id NOT IN (
                    SELECT DISTINCT tr.order_id FROM ods."TRANSACTIONS" tr 
                    INNER JOIN ODS.NS_FC_XREF fc ON fc.NS_FC_ID =tr.LOCATION_ID 
                    WHERE tr.TRAN_SUB_TYPE_ID in (16,82) AND fc.FC_TYPE ='hj')) fcr on fcr.order_id = dje.order_id"""


    sql = glSql.createTranAlloc.format(tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype ,ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter, join2=joinclause)
    
    writeConn.db_execute(sql)
    
    sql = glSql.createTranErrors.format(tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype ,ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter,join2=joinclause)

    writeConn.db_execute(sql)
    
    writeConn.db_commit()
    return ruleId ,mapId,category ,location ,nsaccount,transubtypeid ,transubtype ,trantype


def processNSAllocations(rowCn, runid, periodStartDate, periodEndDate,periodname):
        
    ruleId = rowCn.alloc_rule_id
    mapId= rowCn.dje_map_id
    category = rowCn.category
    location = rowCn.location
    nsaccount =rowCn.accountnumber
    accountmap = rowCn.accountmap
    default = rowCn.default_alloc
    transubtypeid = rowCn.tran_sub_type_id 
    transubtype = rowCn.tran_sub_type
    trantype = rowCn.tran_type
    nstran = rowCn.ns_trans_allocation

    print(f"Allocating Account Number : {nsaccount} using allocation rule: {ruleId} with default {default} on location : {location}")
    
    joinfilter =''
    joinclause =''
    filter2=''
    locationclause=''
    if category is not None:
        joinfilter += f"AND dd.LEVEL1 =  mp.category  "
    
    if location is not None:
        joinfilter += f"AND dd.LEVEL1 = {location}"
        filter2 = f"AND gah.LEVEL1_name = {location}"
        joinclause += f"and mp.location = {location}"
        locationclause +=f"and na.location_id = {location}"

    if ruleId in [501,502]:
        joinfilter += f"AND dd.LEVEL1::varchar = coalesce(na.department_id,22)::varchar"
        filter2 += f""" and coalesce(na.department_id,22)::varchar = gah.level1_name::varchar"""
        # joinclause += """left join(select distinct location_ic as fc_id , dept_id from ods.GL_DEPARTMENT_XREF) fcr on fcr.dept_id = na.department_id  """
        print(joinfilter)
        print(filter2)
        

    if accountmap is not None:
        joinclause += f"""           
        LEFT JOIN   (SELECT *
                        FROM ods.GL_ALLOC_DRIVER_DETAIL
                        WHERE rule_id = '{ruleId}'
                        AND level1 IN ('{accountmap}', '40100')) dd  
                    ON dd.rule_id = '{ruleId}' 
                    AND dd.level1 = COALESCE(
                        (SELECT min(level1) 
                            FROM ods.GL_ALLOC_DRIVER_DETAIL 
                            WHERE rule_id = '{ruleId}' 
                            AND level1 = '{accountmap}'), 
                        '40100') """
    else:
        joinclause +=f"""LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id ={ruleId} {joinfilter}
                    """
        
    print('Running CreateNSAlloc')     
    for key, query in glSql.createNSAlloc.items():    
        sql = query.format(tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype,accountnumber = nsaccount, ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter, join2=joinclause, period = periodname )
        print(sql)
        writeConn.db_execute(sql)
    
    print('checking uncation hersr')
    print(default)
    print(nstran)

    if default == 'Y':

        if nstran =='Y':
            print('Running createDefaultAlloc')  
            for key, query in glSql.createDefaultAlloc.items(): 

                sql = query.format(locjoin = locationclause ,join2 = filter2,join = joinclause,ruleId=ruleId,tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, accountmap = accountmap , accountnumber= nsaccount,  period = periodname)
                print(sql)
                writeConn.db_execute(sql)

            print('Running createDefaultNoSkuAlloc')  
            for key, query in glSql.createDefaultNoSkuAlloc.items(): 
                sql = query.format(join2 = filter2,join = joinclause,ruleId=ruleId,tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, accountmap = accountmap , accountnumber= nsaccount,  period = periodname)
                print(sql)
                writeConn.db_execute(sql)
        else:
            print('Running createDefaultAccountAlloc') 
            for key, query in glSql.createDefaultAccountAlloc.items(): 
            
                
                sql = query.format(join2 = filter2,join = joinclause,ruleId=ruleId,tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, accountmap = accountmap , accountnumber= nsaccount,  period = periodname)
                print(sql)
                writeConn.db_execute(sql)

            print('Running createDefaultNoSkuAccountAlloc') 
            for key, query in glSql.createDefaultNoSkuAccountAlloc.items(): 
         

                sql = query.format(join2 = filter2,join = joinclause,ruleId=ruleId,tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, accountmap = accountmap , accountnumber= nsaccount,  period = periodname)
                print(sql)
                writeConn.db_execute(sql)

    elif default == 'N':
        print('Running createNoSkuAlloc') 
        for key, query in glSql.createNoSkuAlloc.items():                
            
            sql = query.format(tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, runid = runid , accountnumber= nsaccount,endDate=periodEndDate , period = periodname)
            print(sql)
            writeConn.db_execute(sql)



    
    writeConn.db_commit()
    return ruleId ,mapId,category ,location ,nsaccount,transubtypeid ,transubtype ,trantype


def processPostAllocations( periodStartDate, periodEndDate):  
  
    
    print('Creating OOB')
    sql = glSql.createTranOob
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)
    
    # print(sql)
    writeConn.db_execute(sql)


    print('Updating Rounding')
    sql = glSql.updateTranAlloc 
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)
    # print(sql)
    writeConn.db_execute(sql)
    
    
    sql = glSql.createAccountTranOob 
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)
    # print(sql)
    writeConn.db_execute(sql)
    
    sql = glSql.updateAccountTranAlloc 
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)
    # print(sql)
    writeConn.db_execute(sql)
    


def processPostAllocationsNS(rowCn,  periodEndDate,periodname):  
    nsaccount = rowCn.accountnumber

    writeConn.db_execute(f"""update ods.ns_alloc
                          set period_end_date = '{periodEndDate}'
                            where period_end_date is null""")


    sql = glSql.createNSOob
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format(nsaccount=nsaccount,  endDate=periodEndDate , period= periodname)

    print(sql)
    writeConn.db_execute(sql)

    sql = glSql.updateNSAlloc
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format(nsaccount=nsaccount,endDate=periodEndDate,period = periodname)

    print(sql)
    writeConn.db_execute(sql)

    writeConn.db_commit()

def processRules():
    global readConn, writeConn
    readConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_stg')
    writeConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_stg')
    
    writeConn.db_execute("""truncate ods.GL_ALLOC_DRIVER_HEADER""")
    writeConn.db_execute("""truncate ods.GL_ALLOC_DRIVER_DETAIL""")
    writeConn.db_execute("""truncate ods.inbnd_freight_costs""")


    period, periodStartDate, periodEndDate,periodname = getPeriod()
    rows = getRules()

    
    

  
    sql = getattr(glSql, 'getInboundFreight')

    sql = sql.format( endDate=periodEndDate )


    print(sql)

    writeConn.db_execute(sql, commit=True)


    writeConn.db_execute(f""" delete from ods.freight_costs_log
                         where period_end_date = '{periodEndDate}'""")


    writeConn.db_execute(f""" insert into ods.freight_costs_log
                         select *,'{periodEndDate}' from ods.inbnd_freight_costs""")

    for row in rows:

        rowCn = readConn.columnName(row)
        status = processGlAllocRules(rowCn, period, periodStartDate, periodEndDate)

    writeConn.db_execute(f""" delete from ods.GL_ALLOC_DRIVER_HEADER_LOG
                         where period_end_date = '{periodEndDate}'""")
    writeConn.db_execute(f"""delete from ods.GL_ALLOC_DRIVER_DETAIL_LOG
                         where period_end_date = '{periodEndDate}'""")
 
    writeConn.db_execute(""" insert into ods.GL_ALLOC_DRIVER_HEADER_LOG
                         select * from ods.GL_ALLOC_DRIVER_HEADER""")
    writeConn.db_execute("""insert into ods.GL_ALLOC_DRIVER_DETAIL_LOG
                        select dd.*,eh.PERIOD_END_DATE from ods.gl_alloc_driver_detail dd
                        INNER JOIN ods.GL_ALLOC_DRIVER_HEADER eh ON eh.ID =dd.GAH_ID """)   


def processAllocation():
    global readConn, writeConn
    readConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_stg')
    writeConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_stg')

    period, periodStartDate, periodEndDate,periodname = getPeriod()
    
    writeConn.db_execute(f""" 
                        delete FROM ods.gl_alloc gl
                        USING ods.cal_lu cl 
                        where cl.FULLDATE =gl.TRAN_DATE 
                        AND cl.month_end_date = '{periodEndDate}'
                        """)

    rows = getMapODS()

    for row in rows:
        rowCn = readConn.columnName(row)
        status = processGlAllocations(rowCn,  periodStartDate, periodEndDate)
    
    sql = glSql.catchAllocation 
    sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)
    writeConn.db_execute(sql)    

def processAllocationsNS():
    global readConn, writeConn
    readConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_stg')
    writeConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_stg')

    period, periodStartDate, periodEndDate,periodname = getPeriod()
        
    writeConn.db_execute(f""" 
                        delete FROM ods.ns_alloc ns
                        where ns.period_end_date = '{periodEndDate}'
                        """)
    
    writeConn.db_execute(f"""UPDATE ods.NS_CM_GL_ACTIVITY 
                SET TRAN_ALLOCATED = 'N'
                WHERE periodname = '{periodname}'""")
    

    runid = getRun()
    rows = getMapNS()


    for row in rows:
        rowCn = readConn.columnName(row)
        status = processNSAllocations(rowCn ,runid,periodStartDate, periodEndDate, periodname)
        status = processPostAllocationsNS(rowCn,periodEndDate,periodname)


def processRounding():
    global readConn, writeConn
    readConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_stg')
    writeConn = db_connect(dag=DAG, conn_type='snowflake', conn_id='snowflake_ods_stg')

    period, periodStartDate, periodEndDate,periodname = getPeriod()
    

    status = processPostAllocations(periodStartDate, periodEndDate)





with DAG('ods_gl_allocations_main_prd_data', default_args=default_args,
          schedule_interval=None, catchup=False, max_active_runs=1) as dag:


    spProcessRules = PythonOperator(dag=dag,
                                    task_id='process_allocation_rules',
                                    python_callable=processRules,
                                    provide_context=True,
                                    op_kwargs={})

    spProcessAllocation = PythonOperator(dag=dag,
                                    task_id='process_allocations_ods',
                                    python_callable=processAllocation,
                                    provide_context=True,
                                    op_kwargs={})
    
    spProcessNSAllocation = PythonOperator(dag=dag,
                                    task_id='process_allocations_ns',
                                    python_callable=processAllocationsNS,
                                    provide_context=True,
                                    op_kwargs={})
    
    spProcessODSRounding = PythonOperator(dag=dag,
                                    task_id='process_ODS_rounding',
                                    python_callable=processRounding,
                                    provide_context=True,
                                    op_kwargs={})


spProcessRules >> spProcessAllocation >> spProcessNSAllocation >> spProcessODSRounding

# spProcessRules >> spProcessAllocation >> spProcessODSRounding