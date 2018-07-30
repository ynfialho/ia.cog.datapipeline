# -*- coding: utf-8 -*-
from util.db_utils import insert_stg, config_log, stg_tables
import glob
import os
import csv
import pymssql
import json


logger = config_log()
config = json.loads(open('conf.json').read())

def main():
    logger.info("============== START ==============")

    conn = pymssql.connect(host=config['server'], \
    user=config['user'], password=config['pw'], database=config['db'], autocommit=True)

    cursor = conn.cursor()
    files_prefix = ['comp_boss', 'bill_of_', 'price']
    error = []

    cursor.execute("TRUNCATE TABLE stg_component")
    cursor.execute("TRUNCATE TABLE stg_materials")
    cursor.execute("TRUNCATE TABLE stg_price_quote")

    for tbl in files_prefix:
        files_local = glob.glob(os.path.join('', f"./data/{tbl}*"))

        for file in files_local:
            with open(file) as f:
                archive = csv.reader(f)
                columns = next(archive)
                list_csv = list(map(tuple, archive))

                try:
                    logger.info("Inserting in table {}.".format(stg_tables[tbl]))
                    cursor.executemany(insert_stg(stg_tables[tbl], columns), list_csv)
                    logger.info("Insert finish.")
                except Exception as e:
                    error.append(stg_tables[tbl])
                    print(e)
                    continue

    logger.info("Error in tables: {}.".format(', '.join(error) ))

if __name__ == '__main__':
    main()