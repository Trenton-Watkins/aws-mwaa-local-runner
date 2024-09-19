import snowflake.connector
import genGlAllocationDriversSql as glSql
from snowCreds import creds

class ColumnName(object):
    def __init__(self, cursor, row):
        for (attr, val) in zip((d[0] for d in cursor.description), row):
            setattr(self, attr.lower(), val)

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
read_cursor.execute(sql)
row = read_cursor.fetchone()
cn = ColumnName(read_cursor, row)
period = cn.calendaryearmonth
periodStartDate = cn.month_start_date
periodEndDate = cn.month_end_date
periodname = cn.quartertext


sql = glSql.getRun
read_cursor.execute(sql)
row = read_cursor.fetchone()
cn = ColumnName(read_cursor, row)
runid = cn.run_id + 1


sql= glSql.getMapODS
a = read_cursor.execute(sql)

rows = read_cursor.fetchall()

print(f"Beginning Run {runid}")

for row in rows:
    cn = ColumnName(read_cursor, row)
    ruleId = cn.alloc_rule_id
    mapId= cn.dje_map_id
    category = cn.category
    location = cn.location
    nsaccount =cn.accountnumber
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

    sql = sql.format(ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter, join2=joinclause)
    # print(sql)
    write_cursor.execute(sql)

    
    sql = glSql.createTranErrors

    sql = sql.format(ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter,join2=joinclause)
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


sql = glSql.catchAllocation
# sql = getattr(glSql, sqlAttribute + 'Detail')

sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)

# print(sql)
write_cursor.execute(sql)


sql = glSql.createTranOob
# sql = getattr(glSql, sqlAttribute + 'Detail')

sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)

# print(sql)
write_cursor.execute(sql)

sql = glSql.updateTranAlloc
# sql = getattr(glSql, sqlAttribute + 'Detail')

# sql = sql.format(ruleId=ruleId, tranType=tranType, tranSubType=tranSubType, startDate=periodStartDate, endDate=periodEndDate,
#                  tranColumn=tranColumn)
# print(sql)
write_cursor.execute(sql)

sql = glSql.insertToGlAllocation
# sql = getattr(glSql, sqlAttribute + 'Detail')

sql = sql.format(runid=runid)
# print(sql)
write_cursor.execute(sql)



sql= glSql.getMapNS
a = read_cursor.execute(sql)

rows = read_cursor.fetchall()

print(f"Beginning Run {runid}")

for row in rows:
    cn = ColumnName(read_cursor, row)
    ruleId = cn.alloc_rule_id
    mapId= cn.dje_map_id
    category = cn.category
    location = cn.location
    nsaccount =cn.accountnumber
    accountmap = cn.accountmap
    default = cn.default_alloc
    # print(category)
    # print(location)
    print(f"Allocating Account Number : {nsaccount} using allocation rule: {ruleId} with default {default}")
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

  



    if default == 'Y':
                
        sql = glSql.createDefaultAlloc

        sql = sql.format(accountmap = accountmap , accountnumber= nsaccount,  period = periodname)
        # print(sql)
        write_cursor.execute(sql)

    elif default == 'N':
                        
        sql = glSql.createNoSkuAlloc

        sql = sql.format(runid = runid , accountnumber= nsaccount,endDate=periodEndDate , period = periodname)
        # print(sql)
        write_cursor.execute(sql)

    
    for key, query in glSql.createNSAlloc.items():    
        sql = query.format(accountnumber = nsaccount, ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter, join2=joinclause, period = periodname )
        # print(sql)
        write_cursor.execute(sql)





sql = glSql.createNSOob
# sql = getattr(glSql, sqlAttribute + 'Detail')

# sql = sql.format( startDate=periodStartDate, endDate=periodEndDate)

# print(sql)
write_cursor.execute(sql)

sql = glSql.updateNSAlloc
# sql = getattr(glSql, sqlAttribute + 'Detail')

# sql = sql.format(ruleId=ruleId, tranType=tranType, tranSubType=tranSubType, startDate=periodStartDate, endDate=periodEndDate,
#                  tranColumn=tranColumn)
# print(sql)
write_cursor.execute(sql)

sql = glSql.insertNSToGlAllocation
# sql = getattr(glSql, sqlAttribute + 'Detail')

sql = sql.format(runid=runid,startDate=periodStartDate, endDate=periodEndDate)
# print(sql)
write_cursor.execute(sql)





snow_conn.commit()
