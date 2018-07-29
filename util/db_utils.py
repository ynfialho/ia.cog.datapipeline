# -*- coding: utf-8 -*-
# import pyodbc
from multiprocessing.pool import ThreadPool
import time
import logging
from sys import stdout

stg_tables = {"comp_boss": "stg_component",
"bill_of_": "stg_materials",
"price": "stg_price_quote"}

# def rds_mssql_conn(user, pw, server, db, driver = 'PostgreSQL Unicode'):
#     conn = pyodbc.connect('DRIVER={};\
#     SERVER={};\
#     DATABASE={};UID={};PWD={}'.format(driver, server, db, user, pw), autocommit=True)
#     return conn


def insert_stg(table, clms):
    q = """ INSERT INTO dbo.{} VALUES({}) """.format(table, ','.join('?' * len(clms)) )
    return q.replace('?', '%s')

def config_log():
    stdout_handler = logging.StreamHandler(stdout)
    handlers = [stdout_handler]
    logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=handlers)
    logger = logging.getLogger('ETL_CG')
    return logger