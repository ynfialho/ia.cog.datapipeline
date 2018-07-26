import sqlalchemy as sa

def rds_mssql_conn(user, pw, server, db, autocmt = True):
    engine = sa.create_engine("mssql+pyodbc://{}:{}@{}/{}?driver=SQL Server Native Client 11.0;".format(user, pw, server, db), \
    connect_args = {'autocommit':autocmt})

    return engine