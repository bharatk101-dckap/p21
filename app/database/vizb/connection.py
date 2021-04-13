from psycopg2._psycopg import OperationalError
from psycopg2 import connect, extensions, sql
from config import DevelopmentConfig as DB
from app.database.vizb.query import create_table_query

def create_db():
    # declare a new PostgreSQL connection object
    conn = connect(
        dbname=DB.DB_NAME,
        user=DB.DB_USER,
        host=DB.DB_HOST,
        password=DB.DB_PASSWORD
    )
    # get the isolation leve for autocommit
    autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT
    # set the isolation level for the connection's cursors
    # will raise ActiveSqlTransaction exception otherwise
    conn.set_isolation_level(autocommit)
    # instantiate a cursor object from the connection
    cursor = conn.cursor()
    cursor.execute(sql.SQL(
        "CREATE DATABASE {}"
    ).format(sql.Identifier(DB.DB_NAME)))
    conn.commit()
    conn.close()


def connect_db():
    conn = connect(
        dbname=DB.DB_NAME,
        user=DB.DB_USER,
        host=DB.DB_HOST,
        password=DB.DB_PASSWORD
    )
    autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT
    # set the isolation level for the connection's cursors
    # will raise ActiveSqlTransaction exception otherwise
    conn.set_isolation_level(autocommit)
    # instantiate a cursor object from the connection
    cursor = conn.cursor()
    return cursor, conn


def db_v_connect():
    """
    Connects to vizb database and creates if doesn't exists
    :return: Cursor and connection
    """
    try:
        cursor, conn = connect_db()

    except OperationalError:
        create_db()
        cursor, conn = connect_db()

    return cursor, conn


def load_staging_tables(cur, conn):
    for query in create_table_query:
        cur.execute(query)
        conn.commit()
