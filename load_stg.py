# -*- coding: utf-8 -*-
from util.db_utils import rds_mssql_conn, insert_stg, stg_tables, insert_parallel
import boto3
from io import BytesIO
from aws_functions import s3_list_keys, s3_download_single_file
import tempfile
import glob
import os
import csv
import logging
from sys import stdout


def config_log():
    stdout_handler = logging.StreamHandler(stdout)
    handlers = [stdout_handler]
    logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=handlers)
    logger = logging.getLogger('ETL_CG')
    return logger


logger = config_log()
s3_resource = boto3.resource('s3') 
s3_client = boto3.client('s3')
bucket_upload = 'cg-upload'
prefix_upload = 'data/'
server_db = os.environ('server_db')
db_db = os.environ.get('db_db')
user_db = os.environ('user_db')
pw_db = os.environ('pw_db')


def lambda_handler(event, context):
    conn = rds_mssql_conn(server= server_db,\
    user= user_db, pw=pw_db,db= db_db)
    cursor = conn.cursor()

    files_prefix = ['comp_boss', 'bill_of_', 'price']


    tmp = tempfile.NamedTemporaryFile()


    logger.info("List keys in bucket {}".format(bucket_upload))
    obj_list = s3_list_keys(s3_client, bucket_upload, prefix_upload)
    obj_list_prefix = [key for key in obj_list if [True for i in files_prefix if i in key]]

    logger.info("Download keys.")
    list(map(lambda x: \
    s3_download_single_file(s3_resource, bucket_upload, prefix_upload, x.replace('data/',''), tmp.name), \
    obj_list_prefix)) 

    cursor.execute("TRUNCATE TABLE stg_component")
    cursor.execute("TRUNCATE TABLE stg_materials")
    cursor.execute("TRUNCATE TABLE stg_price_quote")

    for tbl in files_prefix:
        files_local = glob.glob(os.path.join('', tmp.name + f"{tbl}*"))

        with open(files_local[0]) as f:
            arq = csv.reader(f)
            columns = next(arq)

            logger.info("Inserting in table {}.".format(stg_tables[tbl]))
            for d in arq:
                cursor.execute(insert_stg(stg_tables[tbl], columns), d)
            #insert_parallel(cursor, tbl, columns, arq)

            logger.info(f"Finish.")


    








#while True:
    # row = cursor.fetchone()
    # if not row:
    #     break
    # print(row)


