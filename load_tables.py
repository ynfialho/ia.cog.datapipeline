# -*- coding: utf-8 -*-
from util.db_utils import insert_stg, config_log, stg_tables
import pymssql
import json

logger = config_log()
config = json.loads(open('conf.json').read())

def main():
    conn = pymssql.connect(host=config['server'], \
    user=config['user'], password=config['pw'], database=config['db'], autocommit=True)

    cursor = conn.cursor()

    tbls = ['component_type', 'connection_type', 'component', 'tube_assembly', 'materials', 'supplier', 'price_quote']
    error = []

    for tb in tbls:
        query = open(f'./querys/load_{tb}.sql').read().replace('\n', ' ')

        logger.info("Inserting in table {}.".format(tb))
        try:
            cursor.execute(query)
            logger.info("Finish insert.")
        except Exception as e:
            error.append(tb)
            print(e)
            continue

    logger.info("Error in tables: {}.".format(', '.join(error) ))

if __name__ == '__main__':
    main()