def dtype_mapping():
    """Create a mapping of df dtypes to mysql data types (not perfect, but close enough)"""
    return {'object': 'TEXT',
            'int64': 'BIGINT',
            'float64': 'FLOAT',
            'datetime64': 'TIMESTAMP',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN',
            'category': 'TEXT',
            'timedelta[ns]': 'TEXT',
            'int32': 'BIGINT',
            'int64': 'BIGINT',
            'float32': 'FLOAT',
            'float64': 'FLOAT'}


def gen_create_sql(df, primary_key, is_primary):
    """
    Creates string with column names, datatype and primary key
    :param df: Dataframe
    :return: create table sql query
    """
    dmap = dtype_mapping()
    sql = ""
    df1 = df.rename(columns={"": "nocolname"})
    hdrs = df1.dtypes.index
    hdrs_list = [(hdr, str(df1[hdr].dtype)) for hdr in hdrs]
    for i, hl in enumerate(hdrs_list):
        sql += "{0} {1},".format(hl[0], dmap[hl[1]])
    if is_primary:
        sql += "PRIMARY KEY ({})".format(primary_key)
    else:
        sql = sql.rstrip(',')
    return sql


def create_sql_tbl_schema(df, tbl_name, primary_key, is_primary):
    """
    Creates SQL Query for table creation
    :param df: dataframe
    :param tbl_name: table name
    :param primary_key: primary key column name
    :return:
    """
    tbl_cols_sql = gen_create_sql(df, primary_key, is_primary)
    #     sql = "USE {0}; CREATE TABLE {1} ({2})".format(db, tbl_name, tbl_cols_sql)
    sql = "CREATE TABLE  IF NOT EXISTS {0} ({1})".format(tbl_name, tbl_cols_sql)
    return sql


def gen_insert_sql(df, table_name):
    """
    Generate SQL Inseert query for dataframe
    :param df: Dataframe
    :param table_name: table name
    :return:
    """
    sql = ''
    df1 = df.rename(columns={"": "nocolname"})
    hdrs = df1.dtypes.index
    hdrs_list = [(hdr, str(df1[hdr].dtype)) for hdr in hdrs]
    for i, hl in enumerate(hdrs_list):
        sql += "{0},".format(hl[0])
    sql = sql.rstrip(',')
    num_columns = len(df.columns)
    return "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING;".format(table_name, sql, ','.join(
        ['%s' for s in range(num_columns)]))
