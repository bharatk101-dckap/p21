import datetime
import threading
from app.automation.scheduler import start
from starlette.background import BackgroundTasks
from starlette.responses import RedirectResponse

from app.modules.common import p21_register, p21_input
from app.mysql_connector.my_sql import create_connection, get_orders, recent_order
from app.database.vizb.connection import db_v_connect
from fastapi import APIRouter, Header, Depends
import pandas as pd
import pandas.io.sql as sqlio
from typing import Optional
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from app.database.data.psql import psql_to_df, df_to_psql, db_connect

import time

from app.auth import verify_token

p21 = APIRouter()
now = datetime.datetime.now()


def df_split(df, chunk):
    dfs = []
    if df.shape[0] > chunk:
        rem = df.shape[0] % chunk
        whole = df.shape[0] - rem
        r = 0
        for i in range(1, int(whole / chunk) + 1):
            m = i * chunk
            z = (r, m)
            dfs.append(df.loc[r:m])
            r = m
        dfs.append(df.loc[m:m + rem])
    return dfs


@p21.get('/')
async def index():
    response = RedirectResponse(url='/docs')
    return response


@p21.post('/p21-register', dependencies=[Depends(verify_token)])
# async def p21_register(filters: p21_register):
#     # try:
#     company_id = filters.company_id
#     print(company_id)
#     server = filters.server
#     port = filters.port
#     database = filters.database
#     user = filters.user
#     password = filters.password
#     print(filters)
#     cursor, conn = db_v_connect()
#     # query1 = "select * from information_schema.tables where table_schema = 'public'"
#     # df = sqlio.read_sql_query(query1, conn)
#     # print(df)
#     query = "INSERT INTO p21 (company_id, server_id, port_id, database_id, user_id, password) " \
#             "VALUES ('{}', '{}', '{}', '{}', '{}','{}') ON CONFLICT DO NOTHING;".format(company_id, server,
#                                                                                         port, database, user, password)
#     conn, status = create_connection(server, port, database, user, password)
#     print(status)
#     if status == 'successful':
#         cursor.execute(query)
async def p21_register(filters: p21_register, company_Id: str = Header(None)):
    try:
        company_id = company_Id
        server = filters.server
        port = filters.port
        database = filters.database
        user = filters.user
        password = filters.password
        cursor, conn = db_v_connect()
        query = "INSERT INTO p21 (company_id, server_id, port_id, database_id, user_id, password) " \
                "VALUES ('{}', '{}', '{}', '{}', '{}','{}') ON CONFLICT (company_id) DO " \
                "UPDATE set server_id='{}', port_id='{}', database_id='{}', " \
                "user_id='{}', password='{}';".format(company_id, server, port, database, user, password,
                                                      server, port, database, user, password)
        conn, status = create_connection(server, port, database, user, password)

        if status == 'successful':
            cursor.execute(query)
            conn.commit()
            conn.close()
            return JSONResponse({"message": "Successful", "status": 200}, status_code=200)
        else:
            return JSONResponse({"message": "Check your Credentials", "status": 500}, status_code=500)
    except Exception:
        return JSONResponse({"message": "Unauthorized, check your credentials", "status": 500}, status_code=500)


@p21.post('/p21-data-pull', dependencies=[Depends(verify_token)])
async def p21_data(background_tasks: BackgroundTasks,company_Id: str = Header(None)):
    def start():
        try:
            company_id = company_Id
            query = "select * from p21 where company_id = '{}';".format(company_id)
            # print(query)
            cursor, conn = db_v_connect()
            df = sqlio.read_sql_query(query, conn)
            server = df['server_id'][0]
            port = df['port_id'][0]
            database = df['database_id'][0]
            user = df['user_id'][0]
            password = df['password'][0]
            # print(server, port, database, user, password)
            order_query = "select oe_hdr.order_no,oe_hdr.date_created,oe_hdr.customer_id,oe_hdr.ship2_name," \
                          "oe_hdr.ship2_email_address,oe_hdr.ship2_add1,oe_hdr.ship2_add2,oe_hdr.ship2_add3," \
                          "oe_hdr.ship2_city,oe_hdr.ship2_country,oe_line.unit_price,oe_line.unit_quantity," \
                          "oe_line.unit_of_measure,oe_hdr.payment_method,oe_line.supplier_id,oe_line.sales_tax," \
                          "oe_line.sales_cost,inv_mast.item_desc,inv_mast.class_id1,inv_mast.class_id2,inv_mast.class_id3," \
                          "inv_mast.class_id4,inv_mast.class_id5,inv_mast.price1,inv_mast.price2," \
                          "inv_mast.price3,inv_mast.price4,inv_mast.price5,inv_mast.price6,inv_mast.price7," \
                          "inv_mast.price8,inv_mast.price9,inv_mast.price10 " \
                          "from oe_line inner join oe_hdr on oe_line.order_no = oe_hdr.order_no left " \
                          "join inv_mast on inv_mast.inv_mast_uid = oe_line.inv_mast_uid"

            df = get_orders(order_query, server, port, database, user, password)
            # df.drop(columns=['Unnamed: 0','class_id3.1'], inplace=True)
            df.reset_index(inplace=True)
            print(df.shape)
            print(df.dtypes)
            dfs = df_split(df, 100000)
            for i in dfs:
                i["price"] = i["unit_price"] * i["unit_quantity"]
                i["date_created"] = pd.to_datetime(i["date_created"])
                i.rename(columns={"class_id3.1": "class_id3_1"}, inplace=True)
                i = i[i["date_created"] >= "2017-01-01"]
                df_to_psql(company_id, i, 'raw_orders', primary_key='order_no', drop=False, is_primary=True)
                print(i.shape)
                time.sleep(30)
            conn.commit()
            conn.close()
            background_tasks.add_task(start)
            return JSONResponse({"message": "Successful", "status": 200}, status_code=200)
        except Exception:
            return JSONResponse({"message": "Failed To Pull Data", "status": 203}, status_code=203)


# @p21.post('/p21-data-pull', dependencies=[Depends(verify_token)])
# async def p21_data(filters: p21_input):
#     company_id = filters.company_id
#     query = "select * from p21 where company_id = '{}';".format(company_id)
#     print(query)
#     cursor, conn = db_v_connect()
#     df = sqlio.read_sql_query(query, conn)
#     server = df['server_id'][0]
#     port = df['port_id'][0]
#     database = df['database_id'][0]
#     user = df['user_id'][0]
#     password = df['password'][0]
#     print(server, port, database, user, password)
#     order_query = "select oe_hdr.order_no,oe_hdr.date_created,oe_hdr.customer_id,oe_hdr.ship2_name," \
#                   "oe_hdr.ship2_email_address,oe_hdr.ship2_add1,oe_hdr.ship2_add2,oe_hdr.ship2_add3," \
#                   "oe_hdr.ship2_city,oe_hdr.ship2_country,oe_line.unit_price,oe_line.unit_quantity," \
#                   "oe_line.unit_of_measure,oe_hdr.payment_method,oe_line.supplier_id,oe_line.sales_tax," \
#                   "oe_line.sales_cost,inv_mast.item_desc,inv_mast.class_id1,inv_mast.class_id2,inv_mast.class_id3," \
#                   "inv_mast.class_id4,inv_mast.class_id5,inv_mast.price1,inv_mast.price2," \
#                   "inv_mast.price3,inv_mast.price4,inv_mast.price5,inv_mast.price6,inv_mast.price7," \
#                   "inv_mast.price8,inv_mast.price9,inv_mast.price10 " \
#                   "from oe_line inner join oe_hdr on oe_line.order_no = oe_hdr.order_no left " \
#                   "join inv_mast on inv_mast.inv_mast_uid = oe_line.inv_mast_uid"
#
#     df = get_orders(order_query, server, port, database, user, password)
#     # df.drop(columns=['Unnamed: 0','class_id3.1'], inplace=True)
#
#     df.reset_index(inplace=True)
#     print(df.shape)
#     print(df.dtypes)
#     dfs = df_split(df, 100000)
#     for i in dfs:
#         df_to_psql(company_id, i, 'raw_orders', primary_key='order_no', drop=False, is_primary=True)
#         print(i.shape)
#         time.sleep(30)
#     conn.commit()
#     conn.close()
#     # return "completed"
#     background_tasks.add_task(start)
#     return JSONResponse({"message": "Successful", "status": 200}, status_code=200)


@p21.post('/p21-recent-order', dependencies=[Depends(verify_token)])
async def p21_recent_data(background_tasks: BackgroundTasks,company_Id: str = Header(None)):
    # def start():
    #     try:
        company_id = company_Id
        query = "select * from p21 where company_id = '{}';".format(company_id)
        print(query)
        cursor, conn = db_v_connect()
        df = sqlio.read_sql_query(query, conn)
        server = df['server_id'][0]
        port = df['port_id'][0]
        database = df['database_id'][0]
        user = df['user_id'][0]
        password = df['password'][0]
        recent_order(server, port, database, user, password, company_id, "order_no", "oe_hdr", "raw_orders")
        # background_tasks.add_task(start)
        return JSONResponse({"message": "Successful", "status": 200}, status_code=200)
        # except Exception:
        #     return JSONResponse({"message":"Failed to update Data","status":203},status_code=203)


@p21.post('/p21-product', dependencies=[Depends(verify_token)])
async def p21_product_data(background_tasks: BackgroundTasks,company_Id: str = Header(None)):
    def start():
        try:
            company_id = company_Id
            query = "select * from p21 where company_id = '{}';".format(company_id)
            print(query)
            cursor, conn = db_v_connect()
            df = sqlio.read_sql_query(query, conn)
            server = df['server_id'][0]
            port = df['port_id'][0]
            database = df['database_id'][0]
            user = df['user_id'][0]
            password = df['password'][0]
            print(server, port, database, user, password)
            vendor = "select po_line.po_no,po_line.qty_ordered,po_line.qty_received,po_line.unit_price,po_line.date_created," \
                     "po_line.date_last_modified,po_line.item_description,po_line.unit_of_measure,po_line.contract_number," \
                     "po_hdr.vendor_id,po_hdr.ship2_name,po_hdr.ship2_add1,po_hdr.ship2_add2," \
                     "po_hdr.ship2_city,po_hdr.ship2_state,po_hdr.ship2_country,po_hdr.ship2_zip,po_hdr.location_id," \
                     "vendor.vendor_name from po_line inner join po_hdr on po_line.po_no = po_hdr.po_no left join vendor on " \
                     "vendor.vendor_id = po_hdr.vendor_id "
            df = get_orders(vendor, server, port, database, user, password)
            df.reset_index(inplace=True)
            print(df.shape)
            print(df.head(1))
            dfs = df_split(df, 100000)
            for i in dfs:
                i["price"] = i["unit_price"] * i["qty_ordered"]
                i["date_created"] = pd.to_datetime(i["date_created"])
                i["year"] = i["date_created"].dt.year
                i = i[i["date_created"] >= "2017-01-01"]
                df_to_psql(company_id, i, 'purchase_order', primary_key='index', drop=False, is_primary=True)
                print(i.shape)
                time.sleep(30)
            conn.commit()
            conn.close()
            background_tasks.add_task(start)
            return JSONResponse({"message": "Successful", "status": 200}, status_code=200)
        except Exception:
            return JSONResponse({"message":"Failed to Pull Product Data","Status":203},status_code=203)


@p21.post('/p21-scheduler', dependencies=[Depends(verify_token)])
async def p21_recent():
    c = threading.Thread(target=start)
    c.start()
    return JSONResponse({"message": "Done", "status": "200"}, status_code=200)


# @p21.on_event("startup")
# async def p21_recent():
#     c = threading.Thread(target=start)
#     c.start()
#     return JSONResponse({"message": "Done", "status": "200"}, status_code=200)

