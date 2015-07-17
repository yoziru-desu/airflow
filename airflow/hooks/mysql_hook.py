from datetime import datetime
import logging
from itertools import izip_longest

import MySQLdb

from airflow.hooks.base_hook import BaseHook


class MySqlHook(BaseHook):
    '''
    Interact with MySQL.
    '''

    def __init__(
            self, mysql_conn_id='mysql_default'):
        self.mysql_conn_id = mysql_conn_id

    def get_conn(self):
        """
        Returns a mysql connection object
        """
        conn = self.get_connection(self.mysql_conn_id)
        conn = MySQLdb.connect(
            conn.host,
            conn.login,
            conn.password,
            conn.schema)
        return conn

    def get_records(self, sql):
        '''
        Executes the sql and returns a set of records.
        '''
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def get_pandas_df(self, sql):
        '''
        Executes the sql and returns a pandas dataframe
        '''
        import pandas.io.sql as psql
        conn = self.get_conn()
        df = psql.read_sql(sql, con=conn)
        conn.close()
        return df

    def run(self, sql):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    def create_table(self, table, fields):
        fields_s = ",\n  ".join([k + ' ' + v for k, v in fields.items()])
        sql = ("CREATE TABLE IF NOT EXISTS {table} (\n{fields_s})\n"
               "".format(**locals()))
        self.run(sql)

    def drop_table(self, table):
        self.run("DROP TABLE IF EXISTS {}".format(table))

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        """
        if target_fields:
            nfields = len(target_fields)
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ''
            nfields = len(rows[0])
        valtemp_string = ", ".join(["%s"] * nfields)
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute('SET autocommit = 0')
        conn.commit()
        sql = "INSERT INTO {0} {1} VALUES ({2})".format(
                table,
                target_fields,
                valtemp_string)
        nrows = len(rows)
        if nrows < commit_every:
            cur.executemany(sql, rows)
        else:
            i = 0
            for row_batch in izip_longest(None, *[iter(rows)] * commit_every):
                cur.executemany(sql, [r for r in row_batch if r is not None])
                conn.commit()
                i += 1
                logging.info(
                    "Loaded {i} into {table} rows so far".format(**locals()))
        conn.commit()
        cur.close()
        conn.close()
        logging.info(
            "Done loading. Loaded a total of {nrows} rows".format(**locals()))
