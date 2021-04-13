import csv
import tempfile
import threading
import pandas as pd
import pandas.io.sql as sqlio
from app import db_v_connect
from app.database.data.connection import db_connect
from app.database.data.query_generator import create_sql_tbl_schema, gen_insert_sql
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import psycopg2.extras as extras
import psycopg2
from app.logs.logs_to_csv import insert_logs
import datetime
from config import DevelopmentConfig
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

register_adapter(dict, Json)


def get_status(company_id):
    query = "select * from data_status where company_id = '{}';".format(company_id)
    df = psql_to_df(company_Id=DevelopmentConfig.DB_NAME, query=query)
    if not df.empty:
        for index, row in df.iterrows():
            updating = row['updating']
    return updating


def update_data_status(company_id, status, timestamp=datetime.datetime.now()):
    """
    Updates data status table
    True if it's updating
    False if it's completed updating
    """
    # print(status)
    cursor, conn = db_v_connect()
    query = "INSERT INTO data_status(company_id, updating, date) VALUES ('{}', '{}', '{}') " \
            "ON CONFLICT (company_id) DO UPDATE set updating = '{}', date ='{}';".format(company_id,
                                                                                         status, timestamp,
                                                                                         status, timestamp)
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


def remove_dup_columns(frame):
    keep_names = set()
    keep_icols = list()
    for icol, name in enumerate(frame.columns):
        if name not in keep_names:
            keep_names.add(name)
            keep_icols.append(icol)
    return frame.iloc[:, keep_icols]


def df_to_psql(company_Id, df, table_name, primary_key, drop=False, is_primary=True):
    """
    Inserts dataframe to postgresql
    :param company_Id: unique key for each company
    :param df: pandas dataframe
    :param table_name: name of the table
    :param primary_key: primary key column name
    :param drop: Boolean, drops table if exists
    :return:
    """

    # def run(company_Id, df, table_name, primary_key, drop, is_primary):
    cur, conn = db_connect(company_Id)
    try:
        if drop:
            cur.execute("DROP TABLE IF EXISTS %s;" % table_name)
            conn.commit()
        df = remove_dup_columns(df)
        create_sql = create_sql_tbl_schema(df, table_name, primary_key, is_primary)
        cur.execute(create_sql)
        conn.commit()
        if primary_key:
            df.dropna(subset=[primary_key], inplace=True)
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL quert to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
        try:
            # update_data_status(company_id=company_Id, status=True)
            extras.execute_values(cur, query, tuples)
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()
        else:
            conn.commit()
        # update_data_status(company_id=company_Id, status=False)
        insert_logs(company_id=company_Id, method=table_name, message="completed")
    except Exception as e:
        insert_logs(company_id=company_Id, method=table_name, message=e)
    # c = threading.Thread(target=run, args=(company_Id, df, table_name, primary_key, drop, is_primary))
    # c.start()
    cur.close()
    conn.close()
    # update_data_status(company_id=company_Id, status=False)
    return "Done"


def psql_to_df(company_Id, query):
    """
    Returns data from psql in pandas dataframe format
    :param company_Id: Unique company key/ID
    :param query: SQL Query
    :return:
    """
    cur, conn = db_connect(company_Id)
    # df = sqlio.read_sql_query(query, conn)
    query = query.rstrip(';')
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        #         conn = db_engine.raw_connection()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile, error_bad_lines=False, engine="python")
    cur.close()
    conn.close()
    return df
