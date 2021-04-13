from app.mysql_connector.my_sql import create_connection
import pandas as pd
from app.database.data.psql import psql_to_df, df_to_psql
from prefect import task
from datetime import timedelta
from app.logs.logs_to_csv import scheduler_track


@task
def recent_order(server, port, database, user, password, cid, p21_col, p21_tab, v_tab):
    try:
        conn, status = create_connection(server, port, database, user, password)
        p21_maxno = "select max(" + p21_col + ") from " + p21_tab
        p_recno = int(conn.execute(p21_maxno).fetchval())
        v_maxno = "select max(" + p21_col + ") from " + v_tab
        v_index_query = "select max(index) from raw_orders"
        v_recno = psql_to_df(cid, v_maxno)
        v_index = psql_to_df(cid, v_index_query)
        v_no = int(v_recno['max'][0])
        v_in = v_index['max'][0]
        print(p_recno, v_no)
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
            df_to_psql(cid, rec_df, 'raw_orders', primary_key='index', drop=False, is_primary=True)
            scheduler_track(cid, "recent_orders", "success")
            return 'completed'
        else:
            scheduler_track(cid, "recent_orders", f"p21_order {p_recno} = vizb_order {v_no} no recent data")
    except Exception as e:
        scheduler_track(cid, "recent_orders", e)


@task
def recent_vendor(server, port, database, user, password, cid, p21_vendor_col, p21_vendor_tab, vendor_tab):
    try:
        conn, status = create_connection(server, port, database, user, password)
        p21_maxno = "select max(" + p21_vendor_col + ") from " + p21_vendor_tab
        p_recno = int(conn.execute(p21_maxno).fetchval())
        v_maxno = "select max(" + p21_vendor_col + ") from " + vendor_tab
        v_index_query = "select max(index) from raw_orders"
        v_recno = psql_to_df(cid, v_maxno)
        v_index = psql_to_df(cid, v_index_query)
        v_no = int(v_recno['max'][0])
        v_in = v_index['max'][0]
        print(p_recno, v_no)
        if p_recno > v_no:
            rec_vendor = "select po_line.po_no,po_line.qty_ordered,po_line.qty_received,po_line.unit_price," \
                         "po_line.date_created,po_line.date_last_modified,po_line.item_description," \
                         "po_line.unit_of_measure,po_line.contract_number,po_hdr.vendor_id,po_hdr.ship2_name," \
                         "po_hdr.ship2_add1,po_hdr.ship2_add2,po_hdr.ship2_city,po_hdr.ship2_state," \
                         "po_hdr.ship2_country,po_hdr.ship2_zip,po_hdr.location_id,vendor.vendor_name " \
                         "from po_line inner join po_hdr on po_line.po_no = po_hdr.po_no left join vendor on " \
                         "vendor.vendor_id = po_hdr.vendor_id where po_line.po_no > " + str(v_no)
            rec_df = pd.read_sql_query(rec_vendor, conn)
            length = rec_df.shape[0]
            i_list = []
            for i in range(0, length):
                v_in += 1
                i_list.append(v_in)
            # print(i_list)
            rec_df['index'] = i_list
            rec_df["price"] = rec_df["unit_price"] * rec_df["qty_ordered"]
            rec_df["date_created"] = pd.to_datetime(rec_df["date_created"])
            rec_df["year"] = rec_df["date_created"].dt.year
            df_to_psql(cid, rec_df, 'purchase_order', primary_key='index', drop=False, is_primary=True)
            scheduler_track(cid, "recent_vendor", "success")
            return 'completed'
        else:
            scheduler_track(cid, "recent_vendor", f"p21_po {p_recno} = vizb_po {v_no} no recent data")
    except Exception as e:
        scheduler_track(cid, "recent_vendor", e)
