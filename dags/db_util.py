from airflow import DAG
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime
import psycopg2



class db_connect():
    def __init__(self, conn_type, conn_id, dag):
        ret_var = None
        try:
            print(str(datetime.now().strftime('%d-%b-%Y %H:%M:%S')) + " - Connecting to " + conn_id)
            if conn_type == 'jdbc':
                target_hook = JdbcHook(jdbc_conn_id=conn_id, dag=dag)
            elif conn_type == 'postgres':
                target_hook = PostgresHook(postgres_conn_id=conn_id, dag=dag)
            elif conn_type == 'sqlserver':
                target_hook = MsSqlHook(mssql_conn_id=conn_id, dag=dag)
            elif conn_type == 'snowflake':
                target_hook = SnowflakeHook(snowflake_conn_id=conn_id, dag=dag)
            elif conn_type == 'mysql':
                target_hook = MySqlHook(mysql_conn_id=conn_id, dag=dag)
        except (Exception, psycopg2.Error) as error:
            print("Error connecting to target", error)
            ret_var = 'Connection Error'

        self.conn = target_hook.get_conn()
        self.cursor = self.conn.cursor()
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.dag = dag

        return ret_var

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def getConn(self):
        return self.conn

    @property
    def getCursor(self):
        return self.cursor

    @property
    def getConnInfo(self):
        return {'conn_type': self.conn_type, 'conn_id': self.conn_id}

    @property
    def getRowCount(self):
        return self.cursor.rowcount

    def db_commit(self):
        self.conn.commit()
        return None

    def db_rollback(self):
        self.conn.rollback()

        return None

    def db_execute(self, sql, verbose=True, commit=False):

        if verbose:
            print(sql)
        self.cursor.execute(sql)

        if commit:
            self.conn.commit()

        return None
    def db_getAllRows(self):

        rows = self.cursor.fetchall()

        return rows
    def db_bulk_insert(self):
        pass

    def db_getManyRows(self, rows_to_fetch):

        rows = self.cursor.fetchmany(rows_to_fetch)

        return rows

    def db_getOneRow(self):

        row = self.cursor.fetchone()

        return row

    def columnName(self, row):

        return ColumnName(self.cursor, row)



    def runFunctions(self, sql_dict, verbose=True, commit=False):

        for key in sql_dict:
            sql = sql_dict[key]
            print(str(datetime.now().strftime('%d-%b-%Y %H:%M:%S')) + " - Starting  " + str(key))
            if sql[:2] == '##':
                call_function = sql[2:]
                globals()[call_function]()
            else:
                if verbose:
                    print('\n' + sql)
                self.cursor.execute(sql)
            print(str(datetime.now().strftime('%d-%b-%Y %H:%M:%S')) + " - Completed  " + str(key))

        if commit:
            self.conn.commit()
        ret_var = True

        return ret_var

class ColumnName(object):
    def __init__(self, cursor, row):
        for (attr, val) in zip((d[0] for d in cursor.description), row):
            setattr(self, attr.lower(), val)

