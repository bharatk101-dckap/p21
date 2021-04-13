from app.automation.clean import sales, sales_executive, sales_forecasting
from app.automation.clean import cust_segment, cust_segment_one, cohort_six
from app.automation.clean import cohort_year, summ_rfm, churn_forecasting, orders_forecasting
from app.automation.clean import vendor_spend_analysis, vendor_deals
from app.automation.recent_pulll import recent_order, recent_vendor
import prefect
from prefect import task, Flow, Parameter, mapped, unmapped
from prefect.schedules import Schedule, clocks, filters
from prefect.executors import LocalDaskExecutor
from datetime import timedelta
from prefect.storage import Local
from prefect.run_configs import LocalRun
import os
from config import DevelopmentConfig
import pytz
import datetime
from app.database.data.psql import psql_to_df
prefect.context.config.cloud.check_cancellation_interval = 60.0


def p21_data():
    schedule = Schedule(clocks=[clocks.IntervalClock(timedelta(minutes=5))],
                        filters=[filters.at_time(
                            datetime.time(hour=8, minute=30).replace(tzinfo=pytz.timezone('Asia/Calcutta')))])
    os.system("prefect backend cloud")
    os.system("prefect auth login -t {}".format(DevelopmentConfig.SCHEDULER_USER_KEY))
    os.system("prefect create project p21")

    with Flow("vizb-p21-etl") as flow:

        data = psql_to_df(DevelopmentConfig.DB_NAME, f"SELECT * FROM p21;")
        a = (datetime.datetime.now() - timedelta(hours=8)).date()
        b = a - timedelta(days=400)
        c = "order_no"
        d = "oe_hdr"
        e = "raw_orders"
        f = "po_no"
        g = "po_line"
        h = "purchase_order"
        cred = [[row['company_id'], row['server_id'], row['port_id'], row['database_id'],
                 row['user_id'], row['password']]
                for index, row in data.iterrows()]

        if len(cred) == 1:
            # Parameters
            server = cred[0][1]
            port = cred[0][2]
            database = cred[0][3]
            user = cred[0][4]
            password = cred[0][5]
            cid = cred[0][0]
            p21_col = c
            p21_tab = d
            v_tab = e
            today = str(a)
            lastday = str(b)
            p21_vendor_col = f
            p21_vendor_tab = g
            vendor_tab = h

            # Tasks
            r1 = recent_order(server, port, database, user, password, cid, p21_col, p21_tab, v_tab)
            c1 = sales(cid, r1)
            c2 = sales_executive(cid, c1)
            c3 = sales_forecasting(cid, c1)
            c4 = cohort_six(cid, today, lastday, c1)
            c5 = cohort_year(cid, today, lastday, c1)
            c6 = cust_segment(cid, c1, c2)
            c7 = cust_segment_one(cid, c1, c2)
            c8 = summ_rfm(cid, c2, c6)
            c9 = churn_forecasting(cid, c2)
            c10 = orders_forecasting(cid, c2)
            r2 = recent_vendor(server, port, database, user, password, cid, p21_vendor_col, p21_vendor_tab, vendor_tab)
            c11 = vendor_spend_analysis(cid, r2)
            c12 = vendor_deals(cid, c11)
        elif len(cred) > 1:
            # Parameters
            server = Parameter("server", default=[i[1] for i in cred])
            port = Parameter("port", default=[i[2] for i in cred])
            database = Parameter("database", default=[i[3] for i in cred])
            user = Parameter("user", default=[i[4] for i in cred])
            password = Parameter("password", default=[i[5] for i in cred])
            cid = Parameter("cid", default=[i[0] for i in cred])
            p21_col = Parameter("p21_col", default=[c for i in cred])
            p21_tab = Parameter("p21_tab", default=[d for i in cred])
            v_tab = Parameter("v_tab", default=[e for i in cred])
            today = Parameter("today", default=[str(a) for i in cred])
            lastday = Parameter("lastday", default=[str(b) for i in cred])
            p21_vendor_col = Parameter("p21_vendor_col", default=[f for i in cred])
            p21_vendor_tab = Parameter("p21_vendor_tab", default=[g for i in cred])
            vendor_tab = Parameter("vendor_tab", default=[h for i in cred])

            # Tasks
            r1 = recent_order.map(server, port, database, user, password, cid, p21_col, p21_tab, v_tab)
            c1 = sales.map(cid, r1)
            c2 = sales_executive.map(cid, c1)
            c3 = sales_forecasting.map(cid, c1)
            c4 = cohort_six.map(cid, today, lastday, c1)
            c5 = cohort_year.map(cid, today, lastday, c1)
            c6 = cust_segment.map(cid, c1, c2)
            c7 = cust_segment_one.map(cid, c1, c2)
            c8 = summ_rfm.map(cid, c2, c6)
            c9 = churn_forecasting.map(cid, c2)
            c10 = orders_forecasting.map(cid, c2)
            r2 = recent_vendor.map(server, port, database, user, password, cid,
                                   p21_vendor_col, p21_vendor_tab, vendor_tab)
            c11 = vendor_spend_analysis.map(cid, r2)
            c12 = vendor_deals.map(cid, c11)

    flow.schedule = schedule
    flow.storage = Local()
    flow.run_config = LocalRun()
    flow.executor = LocalDaskExecutor(scheduler="threads", num_workers=16)
    flow.register(project_name="p21")
    os.system("prefect agent local start --token {}".format(DevelopmentConfig.SCHEDULER_RUNNER_KEY))
    # flow.run()


def start():
    p21_data()
