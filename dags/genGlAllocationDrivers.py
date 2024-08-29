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

a = write_cursor.execute('truncate ods.gl_alloc_driver_header')
a = write_cursor.execute('truncate ods.gl_alloc_driver_detail')



sql = glSql.getPeriod
read_cursor.execute(sql)
row = read_cursor.fetchone()
cn = ColumnName(read_cursor, row)
period = cn.calendaryearmonth
periodStartDate = cn.month_start_date
periodEndDate = cn.month_end_date


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

snow_conn.commit()



