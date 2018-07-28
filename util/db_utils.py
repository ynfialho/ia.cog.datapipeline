# -*- coding: utf-8 -*-
import pyodbc
from multiprocessing.pool import ThreadPool
import time

stg_tables = {"comp_boss": "stg_component",
"bill_of_": "stg_materials",
"price": "stg_price_quote"}

def rds_mssql_conn(user, pw, server, db, driver = 'ODBC Driver 17 for SQL Server'):
    conn = pyodbc.connect('DRIVER={};\
    SERVER={};\
    DATABASE={};UID={};PWD={}'.format(driver, server, db, user, pw), autocommit=True)
    return conn


def insert_stg(table, clms):
    q = "INSERT INTO dbo.{} VALUES({})".format(table ,','.join('?' * len(clms)))
    return q

def insert_parallel_wait(pcur ,ptb, pcl, pld):
    time.sleep(0.3)
    pcur.execute(insert_stg(stg_tables[ptb], pcl), pld)


def insert_parallel(cur ,tb, cl, list_data):
    pool = ThreadPool(20)
    result = pool.map(lambda item: insert_parallel_wait(cur,tb, cl, item), list_data )
    pool.close()
    pool.join()
