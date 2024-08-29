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


sql = glSql.getRun
read_cursor.execute(sql)
row = read_cursor.fetchone()
cn = ColumnName(read_cursor, row)
runid = cn.run_id + 1


sql= glSql.getMap
a = read_cursor.execute(sql)

rows = read_cursor.fetchall()

print(f"Beginning Run {runid}")

for row in rows:
    cn = ColumnName(read_cursor, row)
    ruleId = cn.alloc_rule_id
    mapId= cn.dje_map_id
    category = cn.category
    location = cn.location
    # print(category)
    # print(location)
    print(f"Allocating MAP ID : {mapId} using allocation rule: {ruleId}")
    joinfilter =''
    if category is not None:
        joinfilter += f"AND dd.LEVEL2 = '{category}'"
    
    if location is not None:
        joinfilter = f"AND dd.LEVEL2 = '{location}'"

    # print(joinfilter)

    sql = glSql.createTranAlloc

    sql = sql.format(ruleId=ruleId,   startDate=periodStartDate, endDate=periodEndDate,mapId = mapId ,join = joinfilter)
    # print(sql)
    write_cursor.execute(sql)

    sql = glSql.createTranOob
    # sql = getattr(glSql, sqlAttribute + 'Detail')

    # sql = sql.format(ruleId=ruleId, tranType=tranType, tranSubType=tranSubType, startDate=periodStartDate, endDate=periodEndDate,
    #                  tranColumn=tranColumn)
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


    a = write_cursor.execute('drop table gl_alloc')
    a = write_cursor.execute('drop table oob')




snow_conn.commit()
