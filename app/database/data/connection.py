from psycopg2._psycopg import OperationalError
from psycopg2 import connect, extensions, sql
from config import DevelopmentConfig as DB


def create_db(company_Id):
    # declare a new PostgreSQL connection object
    conn = connect(
        dbname="postgres",
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
    ).format(sql.Identifier(company_Id)))
    conn.commit()
    conn.close()


def connect_db(company_Id):
    conn = connect(
        dbname=company_Id,
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


def db_connect(company_Id):
    """
    Connects to company's/users database and creates if doesn't exists
    :return: Cursor and connection
    """
    try:
        cursor, conn = connect_db(company_Id)

    except OperationalError:
        create_db(company_Id)
        cursor, conn = connect_db(company_Id)

    return cursor, conn



