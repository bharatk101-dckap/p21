import pyodbc
import pandas as pd
from app.database.data.psql import psql_to_df,df_to_psql

def create_connection(server,port,database,user, password):
    cnxn = None
    try:
        cred = ("DRIVER={FreeTDS};SERVER="+str(server)+
                ";PORT="+str(port)+
                ";DATABASE="+str(database)+
                ";UID="+str(user)+
                ";PWD="+str(password)+";")
        cnxn = pyodbc.connect(cred)
        status = "successful"
    except Exception as e:
        status = e

    return cnxn,status


def get_orders(query,server,port,database,user, password):
    conn,status = create_connection(server,port,database,user, password)
    sql_query = pd.read_sql_query(query, conn)
    return sql_query


def recent_order(server, port, database, user, password, company_id, p21_col, p21_tab, v_tab):
    conn, status = create_connection(server, port, database, user, password)
    p21_maxno = "select max(" + p21_col + ") from " + p21_tab
    p_recno = int(conn.execute(p21_maxno).fetchval())
    v_maxno = "select max(" + p21_col + ") from " + v_tab
    v_index_query = "select max(index) from raw_orders"
    v_recno = psql_to_df(company_id, v_maxno)
    v_index = psql_to_df(company_id, v_index_query)
    v_no = int(v_recno['max'][0])
    v_in = v_index['max'][0]
    print(v_in)
    print(v_no,type(v_no))
    print(p_recno,type(p_recno))
    if p_recno > v_no:
        rec_order = "select oe_hdr.order_no,oe_hdr.date_created,oe_hdr.customer_id,oe_hdr.ship2_name," \
                    "oe_hdr.ship2_email_address,oe_hdr.ship2_add1,oe_hdr.ship2_add2,oe_hdr.ship2_add3," \
                    "oe_hdr.ship2_city,oe_hdr.ship2_country,oe_line.unit_price,oe_line.unit_quantity," \
                    "oe_line.unit_of_measure,oe_hdr.payment_method,oe_line.supplier_id,oe_line.sales_tax," \
                    "oe_line.sales_cost,inv_mast.item_desc,inv_mast.class_id1,inv_mast.class_id2," \
                    "inv_mast.class_id3,inv_mast.class_id4,inv_mast.class_id5,inv_mast.price1,inv_mast.price2," \
                    "inv_mast.price3,inv_mast.price4,inv_mast.price5,inv_mast.price6,inv_mast.price7," \
                    "inv_mast.price8,inv_mast.price9,inv_mast.price10 from oe_line inner join oe_hdr on " \
                    "oe_line.order_no = oe_hdr.order_no left join inv_mast on inv_mast.inv_mast_uid = " \
                    "oe_line.inv_mast_uid where oe_hdr.order_no >" + str(v_no)
        rec_df = pd.read_sql_query(rec_order, conn)
        length = rec_df.shape[0]
        i_list = []
        for i in range(0, length):
            v_in += 1
            i_list.append(v_in)
        # print(i_list)
        rec_df['index'] = i_list
        rec_df["price"] = rec_df["unit_price"] * rec_df["unit_quantity"]
        rec_df["date_created"] = pd.to_datetime(rec_df["date_created"])
        rec_df.rename(columns={"class_id3.1": "class_id3_1"}, inplace=True)
        rec_df.reset_index(inplace=True)
        print(rec_df.head(1))
        df_to_psql(company_id, rec_df, 'raw_orders', primary_key='index', drop=False, is_primary=True)
    else:
        pass
    return 'completed'



