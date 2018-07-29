# -*- coding: utf-8 -*-
from util.db_utils import insert_stg, config_log, stg_tables
import boto3
import tempfile
import glob
import os
import csv
import pymssql


logger = config_log()

# s3_resource = boto3.resource('s3') 
# s3_client = boto3.client('s3')
bucket_upload = 'cg-upload'
prefix_upload = 'data/'
# server_db = os.environ('server_db')
# db_db = os.environ.get('db_db')
# user_db = os.environ('user_db')
# pw_db = os.environ('pw_db')



logger.info("============== START ==============")



cursor = conn.cursor()
files_prefix = ['comp_boss', 'bill_of_', 'price']


# logger.info("List keys in bucket {}".format(bucket_upload))
# obj_list = s3_list_keys(s3_client, bucket_upload, prefix_upload)
# obj_list_prefix = [key for key in obj_list if [True for i in files_prefix if i in key]]

# logger.info("Download keys.")
# list(map(lambda x: \
# s3_download_single_file(s3_resource, bucket_upload, prefix_upload, x.replace('data/',''), tmp.name), \
# obj_list_prefix)) 

cursor.execute("TRUNCATE TABLE stg_component")
cursor.execute("TRUNCATE TABLE stg_materials")
cursor.execute("TRUNCATE TABLE stg_price_quote")

for tbl in files_prefix:
    files_local = glob.glob(os.path.join('', f"./data/{tbl}*"))

    with open(files_local[0]) as f:
        archive = csv.reader(f)
        columns = next(archive)
        list_csv = list(map(tuple, archive))

        logger.info("Inserting in table {}.".format(stg_tables[tbl]))
        cursor.executemany(insert_stg(stg_tables[tbl], columns), list_csv)
        
        # for d in archive:
        #     cursor.execute(insert_stg(stg_tables[tbl], columns), tuple(d))

        logger.info("Finish.")
