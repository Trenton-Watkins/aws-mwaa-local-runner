import snowflake.connector
import genGlAllocationDriversSql as glSql
from snowCreds import creds
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time 
from tqdm import tqdm

class ColumnName(object):
    def __init__(self, cursor, row):
        for (attr, val) in zip((d[0] for d in cursor.description), row):
            setattr(self, attr.lower(), val)


# Define the start and end dates
start_date = datetime(2024, 7, 1)  # Start from January 1st, 2023
end_date = datetime(2024, 10 , 1)    # End at January 1st, 2024

# Initialize the current date to the start date
current_date = start_date

# Loop through the first day of each month until the end date is reached
while current_date < end_date:
    start_time = time.time()
    print(current_date.strftime('%Y-%m-%d'))
    # Increment the current date by 1 month




    snow_conn = snowflake.connector.connect(
                                            user=creds['user'],
                                            account=creds['account'],
                                            region = creds['region'],
                                            warehouse=creds['warehouse'],
                                            database=creds['database'],
                                            schema =creds['schema'],
                                            role=creds['role'],
                                            password=creds['password'],
                                            autocommit=creds['autocommit'])

    read_cursor = snow_conn.cursor()
    write_cursor = snow_conn.cursor()

    sql = glSql.getPeriod
    sql = sql.format(current_date = current_date)
    read_cursor.execute(sql)
    row = read_cursor.fetchone()
    cn = ColumnName(read_cursor, row)
    period = cn.calendaryearmonth
    periodStartDate = cn.month_start_date
    periodEndDate = cn.month_end_date
    periodname = cn.quartertext

    ##driver creation
    sql= glSql.getRules
    a = read_cursor.execute(sql)

    rows = read_cursor.fetchall()
    for row in rows:
        cn = ColumnName(read_cursor, row)
        ruleId = cn.id
        tranType = cn.tran_type
        tranSubType = cn.tran_sub_type_id
        sqlAttribute = cn.rule_sql
        tranColumn = cn.tran_column

        sql = getattr(glSql, sqlAttribute + 'Header')

        sql = sql.format(ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,
                        tranType=tranType, tranSubType=tranSubType, period=period,  tranColumn=tranColumn)
        print(sql)
        write_cursor.execute(sql)

        sql = getattr(glSql, sqlAttribute + 'Detail')

        sql = sql.format(ruleId=ruleId, tranType=tranType, tranSubType=tranSubType, startDate=periodStartDate, endDate=periodEndDate,
                        tranColumn=tranColumn)
        print(sql)
        write_cursor.execute(sql)

    print(f"Running ODS Allocation for {period}")

    sql = glSql.getRun

    read_cursor.execute(sql)
    row = read_cursor.fetchone()
    cn = ColumnName(read_cursor, row)
    runid = cn.run_id + 1


    sql= glSql.getMapODS
    a = read_cursor.execute(sql)

    rows = read_cursor.fetchall()

    print(f"Beginning Run {runid}")

    for row in tqdm(rows, desc="Processing Rows"):
        cn = ColumnName(read_cursor, row)
        ruleId = cn.alloc_rule_id
        mapId= cn.dje_map_id
        category = cn.category
        location = cn.location
        nsaccount =cn.accountnumber
        transubtypeid = cn.tran_sub_type_id 
        transubtype = cn.tran_sub_type
        trantype = cn.tran_type
        # print(category)
        # print(location)
        print(f"Allocating MAP ID : {mapId} using allocation rule: {ruleId}")
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

        # print(joinfilter)

        sql = glSql.createTranAlloc

        sql = sql.format(tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype ,ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter, join2=joinclause)
        # print(sql)
        write_cursor.execute(sql)

        
        sql = glSql.createTranErrors

        sql = sql.format(tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype ,ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter,join2=joinclause)
        # print(sql)
        write_cursor.execute(sql)

        # sql = glSql.createTranOob
        # # sql = getattr(glSql, sqlAttribute + 'Detail')

        # # sql = sql.format(ruleId=ruleId, tranType=tranType, tranSubType=tranSubType, startDate=periodStartDate, endDate=periodEndDate,
        # #                  tranColumn=tranColumn)
        # print(sql)
        # write_cursor.execute(sql)
        
        # sql = glSql.updateTranAlloc
        # # sql = getattr(glSql, sqlAttribute + 'Detail')

        # # sql = sql.format(ruleId=ruleId, tranType=tranType, tranSubType=tranSubType, startDate=periodStartDate, endDate=periodEndDate,
        # #                  tranColumn=tranColumn)
        # print(sql)
        # write_cursor.execute(sql)

        # sql = glSql.insertToGlAllocation
        # # sql = getattr(glSql, sqlAttribute + 'Detail')

        # sql = sql.format(runid=runid)
        # print(sql)
        # write_cursor.execute(sql)


        # a = write_cursor.execute('drop table gl_alloc')
        # a = write_cursor.execute('drop table oob')

    print('Running Catch All')
    sql = glSql.catchAllocation
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format( tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype ,startDate=periodStartDate, endDate=periodEndDate)

    # print(sql)
    write_cursor.execute(sql)

    print('Creating OOB')
    sql = glSql.createTranOob
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)

    # print(sql)
    write_cursor.execute(sql)

    print('Updating Rounding')
    sql = glSql.updateTranAlloc 
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)
    # print(sql)
    write_cursor.execute(sql)

    # print('Writing To Allocation tAble')
    # # sql = glSql.insertToGlAllocation
    # # # sql = getattr(glSql, sqlAttribute + 'Detail')

    # # sql = sql.format(runid=runid,tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype )
    # # # print(sql)
    # # write_cursor.execute(sql)

    end_time = time.time()
    print(f"{period} took {end_time - start_time} seconds to execute")
    

    snow_conn = snowflake.connector.connect(
                                            user=creds['user'],
                                            account=creds['account'],
                                            region = creds['region'],
                                            warehouse=creds['warehouse'],
                                            database=creds['database'],
                                            schema =creds['schema'],
                                            role=creds['role'],
                                            password=creds['password'],
                                            autocommit=creds['autocommit'])

    read_cursor = snow_conn.cursor()
    write_cursor = snow_conn.cursor()



    sql = glSql.getRun

    read_cursor.execute(sql)
    row = read_cursor.fetchone()
    cn = ColumnName(read_cursor, row)
    runid = cn.run_id + 1


    print(f"Beginning NS Run {runid}")


    sql = glSql.getPeriod
    sql = sql.format(current_date = current_date)
    read_cursor.execute(sql)
    row = read_cursor.fetchone()
    cn = ColumnName(read_cursor, row)
    period = cn.calendaryearmonth
    periodStartDate = cn.month_start_date
    periodEndDate = cn.month_end_date
    periodname = cn.quartertext

    sql= glSql.getMapNS
    a = read_cursor.execute(sql)

    rows = read_cursor.fetchall()

    for row in rows:
        cn = ColumnName(read_cursor, row)
        ruleId = cn.alloc_rule_id
        mapId= cn.dje_map_id
        category = cn.category
        location = cn.location
        nsaccount =cn.accountnumber
        accountmap = cn.accountmap
        default = cn.default_alloc
        transubtypeid = cn.tran_sub_type_id 
        transubtype = cn.tran_sub_type
        trantype = cn.tran_type
        nstran = cn.ns_trans_allocation
        # print(category)
        # print(location)
        print(f"Allocating Account Number : {nsaccount} using allocation rule: {ruleId} with default {default}")
        joinfilter =''
        joinclause =''
        filter2=''
        if category is not None:
            joinfilter += f"AND dd.LEVEL2 =  mp.category  "
        
        if ruleId ==501:
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
            joinclause +=f"""
                        LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id ={ruleId} {joinfilter}
                        """

        # print(joinfilter)

                            #    LEFT JOIN   (SELECT *
                            # 		     FROM ods.GL_ALLOC_DRIVER_DETAIL
                            # 		     WHERE rule_id = 3000
                            # 		       AND level1 IN ('{accountmap}', '40100')) dd  
                            # 		    ON dd.rule_id = 3000 
                            # 		    AND dd.level1 = COALESCE(
                            # 		        (SELECT level1 
                            # 		         FROM ods.GL_ALLOC_DRIVER_DETAIL 
                            # 		         WHERE rule_id = 3000 
                            # 		           AND level1 = '{accountmap}'), 
                            # 		        '40100') 


        
        if default == 'Y':

            if nstran =='Y':
                sql = glSql.createDefaultAlloc

                sql = sql.format(join2 = filter2,join = joinclause,ruleId=ruleId,tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, accountmap = accountmap , accountnumber= nsaccount,  period = periodname)
                print(sql)
                write_cursor.execute(sql)

                sql = glSql.createDefaultNoSkuAlloc

                sql = sql.format(join2 = filter2,join = joinclause,ruleId=ruleId,tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, accountmap = accountmap , accountnumber= nsaccount,  period = periodname)
                print(sql)
                write_cursor.execute(sql)
            else:
                sql = glSql.createDefaultAccountAlloc

                sql = sql.format(join2 = filter2,join = joinclause,ruleId=ruleId,tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, accountmap = accountmap , accountnumber= nsaccount,  period = periodname)
                print(sql)
                write_cursor.execute(sql)

                sql = glSql.createDefaultNoSkuAccountAlloc

                sql = sql.format(join2 = filter2,join = joinclause,ruleId=ruleId,tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, accountmap = accountmap , accountnumber= nsaccount,  period = periodname)
                print(sql)
                write_cursor.execute(sql)

        elif default == 'N':
                            
            sql = glSql.createNoSkuAlloc

            sql = sql.format(tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype, runid = runid , accountnumber= nsaccount,endDate=periodEndDate , period = periodname)
            print(sql)
            write_cursor.execute(sql)

        
        for key, query in glSql.createNSAlloc.items():    
            sql = query.format(tran_type = trantype ,tran_sub_type_id = transubtypeid,tran_sub_type = transubtype,accountnumber = nsaccount, ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter, join2=joinclause, period = periodname )
            print(sql)
            write_cursor.execute(sql)





    sql = glSql.createNSOob
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format(  endDate=periodEndDate)

    print(sql)
    write_cursor.execute(sql)

    sql = glSql.updateNSAlloc
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format(endDate=periodEndDate)

    print(sql)
    write_cursor.execute(sql)

    sql = glSql.insertNSToGlAllocation
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    sql = sql.format(runid=runid,startDate=periodStartDate, endDate=periodEndDate)
    print(sql)
    write_cursor.execute(sql)
    current_date += relativedelta(months=1)




snow_conn.commit()
