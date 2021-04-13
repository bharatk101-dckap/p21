import calendar
import datetime
import numpy as np
import pandas as pd
import ast
import dask.dataframe as dd
from fbprophet import Prophet
import numpy as np
import pycountry
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from app.logs.logs_to_csv import wrangle_track
import os
import glob
from datetime import date
from app.database.data.psql import psql_to_df, df_to_psql
from datetime import timedelta
from pytz import timezone
import pytz
from fastapi import APIRouter, Depends, Header
from app.auth import verify_token
from config import DevelopmentConfig as Config

p21_wrangle = APIRouter()


def change_timezone(data):
    if "created_at" in data.columns:
        data["created_at"] = pd.to_datetime(data["created_at"])
        data["created_at"] = data["created_at"].apply(lambda x: x - timedelta(hours=5))
        # data["created_at"] = data["created_at"].apply(lambda x: x.replace(tzinfo=pytz.UTC))
        data["created_at"] = data["created_at"].apply(lambda x: x - timedelta(hours=3))
        # data["created_at"] = data["created_at"].apply(lambda x: x.replace(tzinfo=timezone('US/Pacific')))
    if "updated_at" in data.columns:
        data["updated_at"] = pd.to_datetime(data["updated_at"])
        data["updated_at"] = data["updated_at"].apply(lambda x: x - timedelta(hours=5))
        # data["updated_at"] = data["updated_at"].apply(lambda x: x.replace(tzinfo=pytz.UTC))
        data["updated_at"] = data["updated_at"].apply(lambda x: x - timedelta(hours=3))
        # data["updated_at"] = data["updated_at"].apply(lambda x: x.replace(tzinfo=timezone('US/Pacific')))
    return data


# helps to create multiple column from the date columns
def add_datepart(df, fldname, drop=True):
    fld = df[fldname]
    if not np.issubdtype(fld.dtype, np.datetime64):
        df[fldname] = fld = pd.to_datetime(fld, infer_datetime_format=True)
    # targ_pre = re.sub('[Dd]ate$', '', fldname)
    for n in ('Year', 'Month', 'Week', 'Day', 'Dayofweek', 'Dayofyear',
            # 'Is_month_end', 'Is_month_start', 'Is_quarter_end', 'Is_quarter_start', 'Is_year_end',
            # 'Is_year_start'
              ):
        df[n] = getattr(fld.dt, n.lower())
    if drop: df.drop(fldname, axis=1, inplace=True)


# used to label the each customers based on the customer behaviour
def f_label(x):
    # print('yes')
    if x == 1:
        val = '1 Order'
    elif x == 2:
        val = '2 Orders'
    elif x == 3:
        val = '3 Orders'
    else:
        val = '4+ Orders'
    return val


# function helps to segment the day time into finite sessions
def f(x):
    if (x > 4) and (x <= 8):
        return 'Early Morning'
    elif (x > 8) and (x <= 12):
        return 'Morning'
    elif (x > 12) and (x <= 16):
        return 'Noon'
    elif (x > 16) and (x <= 20):
        return 'Evening'
    elif (x > 20) and (x <= 24):
        return 'Night'
    elif (x <= 4):
        return 'Late Night'


# In rfm, after creation of rfm data this helps to reverser the rfm value based on the variable characteristics
def rev_order(x):
    if x == 1:
        return 4
    elif x == 2:
        return 3
    elif x == 3:
        return 2
    elif x == 4:
        return 1


# helps to create the country code, if the country code is not available
def c_code(x):
    try:
        return pycountry.subdivisions.lookup(x).code
    except Exception:
        return ""


# helps to create column by matching if the value in the list from the dict intended
def seg_map(df, col, cat_dict):
    cat = []
    for j in df[col]:
        for key, value in cat_dict.items():
            for i in value:
                if i == j:
                    cat.append(key)
    return cat


def cust_avg_dict(df):
    sales = df
    cust_dicts = {}
    sales['just_date'] = pd.to_datetime(sales['just_date']).dt.date
    for i in sales['email'].tolist():
        test = sales[sales['email'] == i][['just_date', 'email']]
        test.sort_values(by=['just_date'], inplace=True)
        # print(test)
        test['order_days'] = np.nan
        test['order_days'].iloc[0] = 0

        for j in range(test.shape[0] - 1):
            # print(j)
            k = j + 1

            # print(k,j)

            a = test['just_date'].iloc[k] - test['just_date'].iloc[j]

            test['order_days'].iloc[k] = a.days
        b = round(test['order_days'].mean(), 0)
        cust_dicts[i] = b
    return cust_dicts


def cust_avg_churn(df):
    print("cust_avg_churn")
    s_orders = df
    s_orders['just_date'] = pd.to_datetime(s_orders['date_created']).dt.date
    max_date = max(s_orders['just_date'])
    sales = s_orders.groupby(['just_date', 'email']).agg({
        'email': 'count',
        'just_date': lambda date: (max_date - date.max()).days
    })
    sales.columns = ['count', 'recent_order']
    sales.reset_index(inplace=True)

    cust = cust_avg_dict(sales)

    sales['avg_days_order'] = sales['email'].map(cust)
    sales['avg_rate'] = 2 * sales['avg_days_order']
    sales['churn_label'] = sales.apply(lambda x: 'churn' if x.recent_order > x.avg_rate else 'active', axis=1)
    success = pd.DataFrame()
    for i in sales['email'].unique().tolist():
        test = sales[(sales['email'] == i)]
        a = test[(test['just_date'] == max(test['just_date']))]['churn_label'].values
        a = a[0]
        #   print(a)
        test['churn_label'] = a
        success = success.append(test, ignore_index=True)

    print("one done")

    return sales


def sales(company_Id):
    try:
        query = "SELECT * FROM raw_orders;"
        orders = psql_to_df(company_Id, query)
        orders.rename(columns={'date_created': 'created_at', 'ship2_email_address':'email', 'item_desc': 'name',
                       'ship2_city': 'city', 'ship2_country': 'country_id', 'unit_quantity': 'qty_ordered',
                       'price': 'grand_total_final', 'order_no': 'order_id', 'payment_method': 'method'}, inplace=True)
        orders = orders[
                ['city', 'country_id', 'created_at', 'email', 'name',
                 'qty_ordered', 'order_id', 'customer_id',
                 'grand_total_final', 'supplier_id', 'unit_price', 'unit_of_measure', 'method']]
        orders.drop_duplicates(keep='first', inplace=True)
        orders = orders[orders['created_at'] != 'None']
        orders = change_timezone(orders)
        orders['created_at'] = dd.to_datetime(orders.created_at, unit='ns')
        # orders_pd = orders[~orders['status'].isin(['closed', 'canceled'])]
        # orders_pd = orders
        # orders['just_date'] = orders['date_created'].dt.date

        df = orders
        # print(df.head(2))

        # df.created_at.dropna(inplace=True)
        add_datepart(df, 'created_at', drop=False)
        df = df[~df.created_at.isnull()]
        df = df[df['email'].notna()]
        df.Month = df.Month.astype(int)
        df['just_date'] = pd.to_datetime(df['created_at'], format='%Y-%m-%d').dt.date
        df['year_month'] = df['created_at'].apply(lambda x: x.strftime('%Y-%m'))
        df['month_abbr'] = df['Month'].apply(lambda x: calendar.month_abbr[x])
        df.Dayofweek = df.Dayofweek.astype(int)
        df['day_abbr'] = df['Dayofweek'].apply(lambda x: calendar.day_abbr[x])
        df['hour'] = pd.DatetimeIndex(df['created_at']).hour
        df['quater'] = pd.to_datetime(df.created_at).dt.quarter
        df['session'] = df['hour'].apply(f)
        df['just_date'] = df['just_date'].astype(str)
        df.rename(columns={"created_at": "date_created", "grand_total_final": "total",
                           "country_id": 'country_iso2'}, inplace=True)
        # df.replace("complete", "Completed", inplace=True)
        df['total'] = df['total'].astype(float)
        df['method'] = df['method'].astype(str)
        df_to_psql(company_Id=company_Id, table_name='sales', df=df, primary_key=None, is_primary=False, drop=True)
        wrangle_track(company_id=company_Id, method='sales', message="Successfully completed")
    except Exception as e:
        print(e)
        wrangle_track(company_id=company_Id, method='sales', message=e)


class Wrangle:

    def __init__(self, company_Id):
        self.company_Id = company_Id
        # self.directory = get_dataDir(self.company_Id)

    def sales_executive(self):
        try:
            query = "SELECT * FROM SALES;"
            orders = psql_to_df(self.company_Id, query)
            orders['created_at'] = orders['date_created'].astype(str)
            orders.drop(index=list(orders["created_at"][orders["created_at"].str.len() < 7].index), inplace=True)
            orders['created_at'] = pd.to_datetime(orders.date_created).dt.date
            # print(orders_pd.shape)
            df = orders.copy()
            # print(df.shape)
            df = df[~df.created_at.isnull()]
            df = df[df['email'].notna()]
            # print(df.shape)
            # orders['just_date'] = orders['date_created'].dt.date

            # df['just_date'] = pd.to_datetime(df['created_at']).dt.date
            df['just_date'] = pd.to_datetime(df['just_date']).dt.date

            def row_map(row):
                if row['created_at'] == row['min']:
                    val = 1
                else:
                    val = 0
                return val

            a = max(df['just_date'])

            def recent_order(x):
                d = (a - x).days
                return d

            def repeat_col(row):
                if row['freq_pur'] == '4+ Orders':
                    val = 1
                else:
                    val = 0
                return val

            def return_customer(x):
                if x.len > 2 and x.avg_diff > x.std_:
                    res = "returned"
                elif x.len == 2 and x.interval > 0:
                    res = "returned"
                else:
                    res = "not_returned"
                return res

            df["year"] = list(pd.DatetimeIndex(df["created_at"]).year)
            df = df.sort_values(by=["email", "year"], ascending=True)
            df["interval"] = df["created_at"].diff()
            df.loc[df.email != df.email.shift(), "interval"] = 0
            df["interval"] = list(pd.TimedeltaIndex(df["interval"]).days)
            dfc = df.groupby('email')['created_at']
            df = df.assign(min=dfc.transform(min), max=dfc.transform(max), len=dfc.transform(len))
            dfi = df.groupby(["email"])["interval"]
            df = df.assign(mean=dfi.transform(np.mean), std_=dfi.transform(np.std) * 1.4, )
            df["std_"].fillna(0, inplace=True)
            df["avg_diff"] = np.abs(df["interval"] - df["mean"])
            df["win_back"] = df.apply(return_customer, axis=1)

            df['new_customer'] = df.apply(row_map, axis=1)
            # create new column based on just date for the purpose of group by month and year
            # df['year_month'] = df['just_date'].apply(lambda x: x.strftime('%Y-%m'))
            df['min_c'] = pd.to_datetime(df['min']).dt.date
            df['max_c'] = pd.to_datetime(df['max']).dt.date
            df['diff'] = df['max_c'] - df['min_c']
            # Extract the number from days
            df['diff'] = df['diff'] / np.timedelta64(1, 'D')
            # generate the column of average days
            df['avg_day'] = df['diff'] / df['len']
            df['avg_metric'] = 2 * df['avg_day']
            df['recent_order'] = df['max_c'].apply(recent_order)
            df['churn_label'] = df.apply(lambda x: 'churn' if x.recent_order > x.avg_metric else 'active', axis=1)
            df['freq_pur'] = df['len'].apply(f_label)
            df['repeat_customer'] = df.apply(repeat_col, axis=1)
            # df.drop(['Year'], axis=1, inplace=True)
            df_to_psql(company_Id=self.company_Id, table_name="sales_summary", df=df,
                       primary_key=None, is_primary=False, drop=True)
            wrangle_track(company_id=self.company_Id, method='Summary', message="Successfully completed")
            return {"CID": self.company_Id, "Status": "Summary Completed"}
        except Exception as e:
            wrangle_track(company_id=self.company_Id, method='Summary', message=e)
            return {"CID": self.company_Id, "Status": e}

    def sales_forecasting(self):
        try:
            query = "SELECT * FROM SALES;"
            sales = psql_to_df(company_Id=self.company_Id, query=query)
            sales['date_created'] = dd.to_datetime(sales.date_created).dt.date
            sales_pd = sales
            max_date = max(sales_pd.date_created)
            new_max = max_date - datetime.timedelta(days=max_date.day)
            sales_pd = sales_pd[sales_pd['date_created'] <= new_max]
            # sales_pd['year_month'] = sales_pd['date_created'].apply(lambda x: x.strftime('%Y-%m'))
            # sales_pd = sales_pd[(sales_pd['status'] == 'Completed')]
            sales_pd = sales_pd[['date_created', 'total']]
            sales_pd.columns = ['ds', 'y']
            sales_pd.y = sales_pd.y.astype(float)
            df = sales_pd.groupby('ds').sum()['y'].reset_index(name='y')
            m = Prophet()
            m.fit(df)
            future = m.make_future_dataframe(periods=90, freq='D')
            forecast = m.predict(future)
            df_to_psql(company_Id=self.company_Id, table_name="sales_forecasting", df=forecast,
                       primary_key="ds", drop=True)
            wrangle_track(company_id=self.company_Id, method='Forecasting', message="Successfully completed")
            return {"CID": self.company_Id, "Status": "Completed"}
        except Exception as e:
            wrangle_track(company_id=self.company_Id, method='Forecasting', message=e)
            return {"CID": self.company_Id, "Status": e}

    def cust_segment(self):
        try:
            df_query = "SELECT customer_id, email, just_date, order_id, total FROM sales;"
            s_df_query = "SELECT email, min_c, max_c FROM sales_summary;"
            # epc_o_query = "SELECT email, next_order_date FROM expected_order_date;"
            df = psql_to_df(self.company_Id, df_query)
            s_df = psql_to_df(self.company_Id, s_df_query)
            # expected_order = psql_to_df(self.company_Id, epc_o_query)
            # expected_order["next_order_date"] = pd.to_datetime(expected_order['next_order_date'])

            s_df.reset_index(inplace=True, drop=True)

            # df = df.compute()
            df['just_date'] = pd.to_datetime(df['just_date']).dt.date
            df = df[['customer_id', 'email', 'just_date', 'order_id', 'total']]
            df_rfm = df[df['customer_id'].notnull() & df['order_id'].notnull()]
            df_rfm = df_rfm[df_rfm['email'].notnull()]
            maxdate = max(df_rfm['just_date']) + datetime.timedelta(days=1)
            rfm = df_rfm.groupby('email').agg({'just_date': lambda date: (maxdate - date.max()).days,
                                               'order_id': lambda num: len(num),
                                               'total': lambda price: price.sum()})
            rfm.columns = ['recency', 'frequency', 'monetary']
            rfm = rfm[rfm['monetary'] != 0]
            # unskew the data
            rfm['frequency_log'] = np.log(rfm['frequency'])
            rfm['monetary_log'] = np.log(rfm['monetary'])
            rfm['recency_log'] = np.log(rfm['recency'])
            # standardization and normalization the data
            clust_rfm = rfm[['recency_log', 'frequency_log', 'monetary_log']]
            scaler = StandardScaler()
            scaler.fit(clust_rfm)
            rfm_normalized = scaler.transform(clust_rfm)
            data_rfm_normalized = pd.DataFrame(rfm_normalized, columns=clust_rfm.columns)
            # iterate the clusters to find the optimized cluster number
            #     sse = {}
            #     for k in range(1, 21):
            #         # Initialize KMeans with k clusters
            #         kmeans = KMeans(n_clusters=k, random_state=1)

            #         # Fit KMeans on the normalized dataset
            #         kmeans.fit(data_rfm_normalized)

            #         # Assign sum of squared distances to k element of dictionary
            #         sse[k] = kmeans.inertia_
            #     # Add the plot title "The Elbow Method"
            #     plt.title('The Elbow Method')

            #     # Add X-axis label "k"
            #     plt.xlabel('k')

            #     # Add Y-axis label "SSE"
            #     plt.ylabel('SSE')

            #     # Plot SSE values for each key in the dictionary
            #     sns.pointplot(x=list(sse.keys()), y=list(sse.values()))
            #     plt.show()

            # Initialize KMeans
            kmeans = KMeans(n_clusters=4, random_state=1)

            # Fit k-means clustering on the normalized data set
            kmeans.fit(data_rfm_normalized)

            # Extract cluster labels
            cluster_labels = kmeans.labels_
            # print(set(cluster_labels))

            # Create a DataFrame by adding a new cluster label column
            data_rfm_k4 = rfm.assign(Cluster=cluster_labels)
            data_rfm_k4.reset_index(inplace=True)
            # print("2")
            rfm = rfm.assign(Cluster=cluster_labels)

            #     # Group the data by cluster
            #     grouped = data_rfm_k4.groupby(['Cluster'])

            #     # Calculate average RFM values and segment sizes per cluster value
            #     grouped.agg({
            #         'recency': 'mean',
            #         'frequency': 'mean',
            #         'monetary': ['mean', 'count']
            #     }).round(1)

            #     #It clearly shows 1 and 3 group people are churn from the business

            r_labels = list(range(4, 0, -1))

            rfm['R'] = pd.qcut(rfm['recency'], 4, r_labels)
            rfm['F'] = pd.qcut(rfm['frequency'].rank(method='first').values, 4, ).codes + 1
            rfm['M'] = pd.qcut(rfm['monetary'].rank(method='first').values, 4, ).codes + 1

            rfm['M'] = rfm['M'].apply(rev_order)
            rfm['F'] = rfm['F'].apply(rev_order)
            rfm['R'] = rfm['R'].apply(rev_order)

            rfm['RFM'] = rfm['R'].astype(str) + rfm['F'].astype(str) + rfm['M'].astype(str)

            rfm['S_RFM'] = rfm['R'].astype(int) + rfm['F'].astype(int) + rfm['M'].astype(int)

            cust_dict = {'High Spending-New Customer': [131, 132, 141, 142, ],
                         'Low Spending-New Customer': [133, 134, 143, 144, ],
                         'High Spending-Loyal Customer': [111, 112, 121, 122, ],
                         'Low Spending-Loyal Customer': [113, 114, 123, 124, ],
                         'High Spending-Potential Loyalist': [211, 212, 221, 222, ],
                         'Low Spending-Potential Loyalist': [213, 214, 223, 224, ],
                         'High Spending-Promising Customer': [231, 232, 241, 242, ],
                         'Low Spending Promising Customer': [233, 234, 243, 244, ],
                         'High Spent-Attention Seekers': [311, 312, 321, 322, ],
                         'Low Spent-Attention Seeker': [313, 314, 323, 324, ],
                         'High Spent-Likely to Churn': [331, 332, 341, 342, ],
                         'Low Spent-Likely to Churn': [333, 334, 343, 344, ],
                         'High Spent-Churn Best Customer': [411, 412, 421, 422, ],
                         'Low Spent-Churn  Best Customer': [413, 414, 423, 424, ],
                         'Churn Customer': [431, 432, 433, 434, 441, 442, 443, 444, ]
                         }

            rfm['RFM_int'] = rfm['RFM'].astype(int)

            rfm['cust_segment'] = seg_map(rfm, 'RFM_int', cust_dict)

            quad_dict = {'Platinum': ['High Spending-Loyal Customer', 'High Spending-Potential Loyalist',
                                      'Low Spending-Loyal Customer'],
                         'Gold': ['High Spending-New Customer', 'Low Spending-New Customer',
                                  'High Spending-Promising Customer', 'Low Spending-Potential Loyalist'],
                         'Silver': ['Low Spending Promising Customer', 'High Spent-Attention Seekers',
                                    'Low Spent-Attention Seeker', 'High Spent-Likely to Churn',
                                    'Low Spent-Likely to Churn'],
                         'Bronze': ['High Spent-Churn Best Customer', 'Low Spent-Churn  Best Customer',
                                    'Churn Customer']
                         }

            rfm['quad_segment'] = seg_map(rfm, 'cust_segment', quad_dict)
            rfm['f_label'] = rfm['frequency'].apply(f_label)
            rfm['avg_order_value'] = rfm['monetary'] / rfm['frequency']
            rfm.reset_index(inplace=True)
            # print('2.5')
            rfm = pd.merge(rfm, s_df, how='inner', on='email', validate='one_to_many')
            # print('3')
            rfm = rfm.drop_duplicates('email')
            rfm.reset_index(inplace=True, drop=True)
            rfm.set_index('email')
            # print('4')
            # rfm_expected_order = pd.merge(rfm, expected_order, on="email", how="left")
            #
            # # rfm_expected_order["Next_order_date"].fillna("None",inplace=True)
            # # print('4')
            # rfm_expected_order = rfm_expected_order.drop_duplicates('email')
            # # print('4')
            # rfm_expected_order.set_index('email')
            # rfm_expected_order.reset_index(inplace=True)
            # rfm_expected_order.next_order_date.replace({np.nan: None}, inplace=True)
            df_to_psql(company_Id=self.company_Id, table_name="rfm", df=rfm, primary_key="email",
                       drop=True)
            wrangle_track(company_id=self.company_Id, method='RFM', message="Successfully completed")
            return {"CID": self.company_Id, "Status": "RFM full Completed"}
        except Exception as e:
            wrangle_track(company_id=self.company_Id, method='RFM', message=e)
            return {"CID": self.company_Id, "Status": e}

    def cust_segment_one(self):
        try:
            df_query = "SELECT customer_id, email, just_date, order_id, total FROM sales;"
            s_df_query = "SELECT email, min_c, max_c FROM sales_summary;"
            # epc_o_query = "SELECT email, next_order_date FROM expected_order_date;"
            df = psql_to_df(self.company_Id, df_query)
            s_df = psql_to_df(self.company_Id, s_df_query)
            # expected_order = psql_to_df(self.company_Id, epc_o_query)
            # expected_order["next_order_date"] = pd.to_datetime(expected_order['next_order_date'])
            s_df.reset_index(inplace=True, drop=True)

            # df = df.compute()
            df['just_date'] = pd.to_datetime(df['just_date']).dt.date
            max_date = max(df['just_date'])
            df = df[df['just_date'] >= (max_date - datetime.timedelta(365))]
            df = df[['customer_id', 'email', 'just_date', 'order_id', 'total']]
            df_rfm = df[df['customer_id'].notnull() & df['order_id'].notnull()]
            df_rfm = df_rfm[df_rfm['email'].notnull()]

            maxdate = max(df_rfm['just_date']) + datetime.timedelta(days=1)
            rfm = df_rfm.groupby('email').agg({'just_date': lambda date: (maxdate - date.max()).days,
                                               'order_id': lambda num: len(num),
                                               'total': lambda price: price.sum()})
            rfm.columns = ['recency', 'frequency', 'monetary']
            rfm = rfm[rfm['monetary'] != 0]

            # unskew the data
            rfm['frequency_log'] = np.log(rfm['frequency'])
            rfm['monetary_log'] = np.log(rfm['monetary'])
            rfm['recency_log'] = np.log(rfm['recency'])
            # standardization and normalization the data
            clust_rfm = rfm[['recency_log', 'frequency_log', 'monetary_log']]
            scaler = StandardScaler()
            scaler.fit(clust_rfm)
            rfm_normalized = scaler.transform(clust_rfm)
            data_rfm_normalized = pd.DataFrame(rfm_normalized, columns=clust_rfm.columns)
            # iterate the clusters to find the optimized cluster number
            #     sse = {}
            #     for k in range(1, 21):
            #         # Initialize KMeans with k clusters
            #         kmeans = KMeans(n_clusters=k, random_state=1)

            #         # Fit KMeans on the normalized dataset
            #         kmeans.fit(data_rfm_normalized)

            #         # Assign sum of squared distances to k element of dictionary
            #         sse[k] = kmeans.inertia_
            #     # Add the plot title "The Elbow Method"
            #     plt.title('The Elbow Method')

            #     # Add X-axis label "k"
            #     plt.xlabel('k')

            #     # Add Y-axis label "SSE"
            #     plt.ylabel('SSE')

            #     # Plot SSE values for each key in the dictionary
            #     sns.pointplot(x=list(sse.keys()), y=list(sse.values()))
            #     plt.show()

            # Initialize KMeans
            kmeans = KMeans(n_clusters=4, random_state=1)

            # Fit k-means clustering on the normalized data set
            kmeans.fit(data_rfm_normalized)

            # Extract cluster labels
            cluster_labels = kmeans.labels_
            # print(set(cluster_labels))

            # Create a DataFrame by adding a new cluster label column
            data_rfm_k4 = rfm.assign(Cluster=cluster_labels)
            data_rfm_k4.reset_index(inplace=True)

            rfm = rfm.assign(Cluster=cluster_labels)

            #     # Group the data by cluster
            #     grouped = data_rfm_k4.groupby(['Cluster'])

            #     # Calculate average RFM values and segment sizes per cluster value
            #     grouped.agg({
            #         'recency': 'mean',
            #         'frequency': 'mean',
            #         'monetary': ['mean', 'count']
            #     }).round(1)

            #     #It clearly shows 1 and 3 group people are churn from the business

            r_labels = list(range(4, 0, -1))

            rfm['R'] = pd.qcut(rfm['recency'], 4, r_labels)
            rfm['F'] = pd.qcut(rfm['frequency'].rank(method='first').values, 4, ).codes + 1
            rfm['M'] = pd.qcut(rfm['monetary'].rank(method='first').values, 4, ).codes + 1

            rfm['M'] = rfm['M'].apply(rev_order)
            rfm['F'] = rfm['F'].apply(rev_order)
            rfm['R'] = rfm['R'].apply(rev_order)

            rfm['RFM'] = rfm['R'].astype(str) + rfm['F'].astype(str) + rfm['M'].astype(str)

            rfm['S_RFM'] = rfm['R'].astype(int) + rfm['F'].astype(int) + rfm['M'].astype(int)

            cust_dict = {'High Spending-New Customer': [131, 132, 141, 142, ],
                         'Low Spending-New Customer': [133, 134, 143, 144, ],
                         'High Spending-Loyal Customer': [111, 112, 121, 122, ],
                         'Low Spending-Loyal Customer': [113, 114, 123, 124, ],
                         'High Spending-Potential Loyalist': [211, 212, 221, 222, ],
                         'Low Spending-Potential Loyalist': [213, 214, 223, 224, ],
                         'High Spending-Promising Customer': [231, 232, 241, 242, ],
                         'Low Spending Promising Customer': [233, 234, 243, 244, ],
                         'High Spent-Attention Seekers': [311, 312, 321, 322, ],
                         'Low Spent-Attention Seeker': [313, 314, 323, 324, ],
                         'High Spent-Likely to Churn': [331, 332, 341, 342, ],
                         'Low Spent-Likely to Churn': [333, 334, 343, 344, ],
                         'High Spent-Churn Best Customer': [411, 412, 421, 422, ],
                         'Low Spent-Churn  Best Customer': [413, 414, 423, 424, ],
                         'Churn Customer': [431, 432, 433, 434, 441, 442, 443, 444, ]
                         }

            rfm['RFM_int'] = rfm['RFM'].astype(int)

            rfm['cust_segment'] = seg_map(rfm, 'RFM_int', cust_dict)

            quad_dict = {'Platinum': ['High Spending-Loyal Customer', 'High Spending-Potential Loyalist',
                                      'Low Spending-Loyal Customer'],
                         'Gold': ['High Spending-New Customer', 'Low Spending-New Customer',
                                  'High Spending-Promising Customer', 'Low Spending-Potential Loyalist'],
                         'Silver': ['Low Spending Promising Customer', 'High Spent-Attention Seekers',
                                    'Low Spent-Attention Seeker', 'High Spent-Likely to Churn',
                                    'Low Spent-Likely to Churn'],
                         'Bronze': ['High Spent-Churn Best Customer', 'Low Spent-Churn  Best Customer',
                                    'Churn Customer']
                         }

            rfm['quad_segment'] = seg_map(rfm, 'cust_segment', quad_dict)
            rfm['f_label'] = rfm['frequency'].apply(f_label)
            rfm['avg_order_value'] = rfm['monetary'] / rfm['frequency']
            rfm.reset_index(inplace=True)
            rfm = pd.merge(rfm, s_df, how='inner', on='email', validate='one_to_many')
            rfm = rfm.drop_duplicates('email')
            rfm.reset_index(inplace=True, drop=True)
            rfm.set_index('email')
            # rfm_expected_order = pd.merge(rfm, expected_order, on="email", how="left")

            # rfm_expected_order["Next_order_date"].fillna("None",inplace=True)

            # rfm_expected_order = rfm_expected_order.drop_duplicates('email')
            # rfm_expected_order.set_index('email')
            #
            # # rfm.to_csv(self.directory + '/cust_segment.csv')
            # rfm_expected_order.reset_index(inplace=True)
            # # rfm_expected_order.next_order_date.replace({'NaT': None}, inplace=True)
            # rfm_expected_order.next_order_date.replace({np.nan: None}, inplace=True)
            df_to_psql(company_Id=self.company_Id, table_name="rfm_one", df=rfm, primary_key="email",
                       drop=True)
            wrangle_track(company_id=self.company_Id, method='RFM One', message="Successfully completed")
            return {"CID": self.company_Id, "Status": "RFM Single Completed"}
        except Exception as e:
            wrangle_track(company_id=self.company_Id, method='RFM One', message=e)
            return {"CID": self.company_Id, "Status": e}

    def cohort_six(self):
        try:
            query = "SELECT order_id, date_created, email, total FROM sales;"
            df = psql_to_df(self.company_Id, query)
            # df = df.compute()
            df['just_date'] = pd.to_datetime(df['date_created']).dt.date
            max_date = max(df.just_date)
            new_max = max_date - datetime.timedelta(days=max_date.day)
            df = df[df['just_date'] <= new_max]
            # print(df.shape)
            df = df[['order_id', 'just_date', 'email', 'total']]
            df = df.dropna(axis=0)
            df = df[df['total'] > 0]
            # df = df.drop(['just_date'], axis=1)
            df['just_date'] = pd.to_datetime(df['just_date']).dt.date
            days = [211, 212, 213]
            for i in days:
                t = max(df['just_date']) - datetime.timedelta(i)
                # print(t.day)
                if t.day == 1:
                    d = max(df['just_date']) - datetime.timedelta(i)

            df1 = df[df['just_date'] >= d]
            # print(df1.shape)
            # print(max(df1['just_date']))
            # print(min(df1['just_date']))
            df1['OrderMonth'] = df1['just_date'].apply(lambda x: datetime.datetime(x.year, x.month, 1))
            df1['CohortMonth'] = df1.groupby('email')['OrderMonth'].transform('min')
            df1['CohortIndex'] = (df1['OrderMonth'].apply(lambda x: x.year) - df1['CohortMonth'].apply(
                lambda x: x.year)) * 360 + \
                                 (df1['OrderMonth'].apply(lambda x: x.month) - df1['CohortMonth'].apply(
                                     lambda x: x.month)) * 30
            df1['CohortMonth'] = df1['CohortMonth'].apply(lambda x: x.strftime('%Y-%m'))
            grouping = df1.groupby(['CohortMonth', 'CohortIndex'])
            cohort_data = grouping.agg({'email': pd.Series.nunique,
                                        'total': np.mean,
                                        })
            cohort_data.rename(columns={'email': 'CustomerCount',
                                        'total': 'Average_Sales'}, inplace=True)
            cohort_data.reset_index(level=['CohortMonth', 'CohortIndex'], inplace=True)
            cohort_counts = cohort_data.pivot(index='CohortMonth', columns='CohortIndex', values='CustomerCount')
            # print(cohort_counts.columns)
            # print(cohort_counts.shape)
            columns = ['New_Customer', 'Days_30', 'Days_60', 'Days_90', 'Days_120', 'Days_150',
                       'Days_180']
            if len(cohort_counts.columns) < 7:
                for i in range(len(cohort_counts.columns), 7):
                    cohort_counts.insert(i, columns[i], np.nan)
            cohort_counts.columns = columns
            # average sales in the cohort
            avg_salesDF = cohort_data.groupby(['CohortMonth']).sum()['Average_Sales'].reset_index(
                name='Average Sales').round(2)
            avg_salesDF.set_index('CohortMonth')
            avg_cohort = cohort_counts.merge(avg_salesDF, left_index=True, right_on='CohortMonth')
            avg_cohort = avg_cohort.set_index('CohortMonth')
            avg_cohort = avg_cohort[
                cohort_counts.columns]
            # retention - from cohort
            cohort_size = cohort_counts.iloc[:, 0]
            retention = cohort_counts.divide(cohort_size, axis=0).round(3) * 100
            retention.columns = cohort_counts.columns
            # print(retention)
            # cohort_counts.to_csv(self.directory + '/cohort_half.csv')
            # retention.to_csv(self.directory + '/retention_half.csv')
            cohort_counts.reset_index(inplace=True)
            df_to_psql(company_Id=self.company_Id, table_name="cohort_half", df=cohort_counts,
                       primary_key='CohortMonth',
                       drop=True)
            retention.reset_index(inplace=True)
            df_to_psql(company_Id=self.company_Id, table_name="retention_half", df=retention,
                       primary_key='CohortMonth', drop=True)
            wrangle_track(company_id=self.company_Id, method='Cohort 6', message="Successfully completed")
            return {"CID": self.company_Id, "Status": "Cohort six Completed"}
        except Exception as e:
            wrangle_track(company_id=self.company_Id, method='Cohort 6', message=e)
            return {"CID": self.company_Id, "Status": e}

    def cohort_year(self):
        try:
            query = "SELECT order_id, date_created, email, total FROM sales;"
            df = psql_to_df(self.company_Id, query)
            # df = df.compute()
            df['just_date'] = pd.to_datetime(df['date_created']).dt.date
            max_date = max(df.just_date)
            new_max = max_date - datetime.timedelta(days=max_date.day)
            df = df[df['just_date'] <= new_max]
            df = df[['order_id', 'just_date', 'email', 'total']]
            df = df.dropna(axis=0)
            df = df[df['total'] > 0]
            # df = df.drop(['just_date'], axis=1)
            df['just_date'] = pd.to_datetime(df['just_date']).dt.date
            days = [395, 396]
            for i in days:
                t = max(df['just_date']) - datetime.timedelta(i)
                if t.day == 1:
                    d = max(df['just_date']) - datetime.timedelta(i)
            # d = max(df['just_date']) - datetime.timedelta(360)
            df1 = df[df['just_date'] >= d]
            df1['OrderMonth'] = df1['just_date'].apply(lambda x: datetime.datetime(x.year, x.month, 1))
            df1['CohortMonth'] = df1.groupby('email')['OrderMonth'].transform('min')
            df1['CohortIndex'] = (df1['OrderMonth'].apply(lambda x: x.year) - df1['CohortMonth'].apply(
                lambda x: x.year)) * 360 + \
                                 (df1['OrderMonth'].apply(lambda x: x.month) - df1['CohortMonth'].apply(
                                     lambda x: x.month)) * 30
            df1['CohortMonth'] = df1['CohortMonth'].apply(lambda x: x.strftime('%Y-%m'))
            grouping = df1.groupby(['CohortMonth', 'CohortIndex'])
            cohort_data = grouping.agg({'email': pd.Series.nunique,
                                        'total': np.mean,
                                        })
            cohort_data.rename(columns={'email': 'CustomerCount',
                                        'total': 'Average_Sales'}, inplace=True)
            cohort_data.reset_index(level=['CohortMonth', 'CohortIndex'], inplace=True)
            cohort_counts = cohort_data.pivot(index='CohortMonth', columns='CohortIndex', values='CustomerCount')
            columns = ['New_Customer', 'Days_30', 'Days_60', 'Days_90', 'Days_120', 'Days_150',
                       'Days_180', 'Days_210', 'Days_240', 'Days_270', 'Days_300', 'Days_330', 'Days_360']
            if len(cohort_counts.columns) < 13:
                for i in range(len(cohort_counts.columns), 13):
                    cohort_counts.insert(i, columns[i], np.nan)
            cohort_counts.columns = columns
            # average sales in the cohort
            avg_salesDF = cohort_data.groupby(['CohortMonth']).sum()['Average_Sales'].reset_index(
                name='Average Sales').round(2)
            avg_salesDF.set_index('CohortMonth')
            avg_cohort = cohort_counts.merge(avg_salesDF, left_index=True, right_on='CohortMonth')
            avg_cohort = avg_cohort.set_index('CohortMonth')
            avg_cohort = avg_cohort[
                cohort_counts.columns]
            # retention - from cohort
            cohort_size = cohort_counts.iloc[:, 0]
            retention = cohort_counts.divide(cohort_size, axis=0).round(3) * 100
            retention.columns = cohort_counts.columns

            # cohort_counts.to_csv(self.directory + '/cohort_year.csv')
            # retention.to_csv(self.directory + '/retention_year.csv')

            cohort_counts.reset_index(inplace=True)
            df_to_psql(company_Id=self.company_Id, table_name="cohort_year", df=cohort_counts,
                       primary_key='CohortMonth', drop=True)
            retention.reset_index(inplace=True)
            df_to_psql(company_Id=self.company_Id, table_name="retention_year", df=retention,
                       primary_key='CohortMonth', drop=True)
            wrangle_track(company_id=self.company_Id, method='Cohort 1', message="Successfully completed")
            return {"CID": self.company_Id, "Status": "Completed"}
        except Exception as e:
            wrangle_track(company_id=self.company_Id, method='Cohort 1', message=e)
            return {"CID": self.company_Id, "Status": e}

    def summ_rfm(self):
        try:
            summ_query = "SELECT * FROM sales_summary;"
            rfm_query = "SELECT email,  cust_segment, quad_segment FROM rfm;"
            summ = psql_to_df(self.company_Id, summ_query)
            rfm = psql_to_df(self.company_Id, rfm_query)
            df = pd.merge(rfm, summ, on='email', how='outer', validate="one_to_many")
            df_to_psql(company_Id=self.company_Id, df = df, table_name="summ_rfm", primary_key=None, is_primary=False,
                       drop=True)
            wrangle_track(company_id=self.company_Id, method='Summary RFM Clean', message="Successfully completed")
            return {"CID": self.company_Id, "Status": "Summary RFM Completed"}

        except Exception as e:
            wrangle_track(company_id=self.company_Id, method='Summary RFM Clean', message=e)
            return {"CID": self.company_Id, "Status": e}

    def churn_forecasting(self):
        """
        """
        try:
            query = "SELECT date_created, churn_label FROM sales_summary;"
            df = psql_to_df(self.company_Id, query)
            df["Year"] = list(pd.DatetimeIndex(df["date_created"]).year)
            df = df[df["churn_label"] == "churn"]
            df["date_created"] = pd.to_datetime(df["date_created"]).dt.date
            df.sort_values(by=["Year", "date_created"], inplace=True)

            grouper = df.groupby(["date_created"])["churn_label"]
            df = pd.DataFrame(grouper.count())
            df.columns = ["churn"]
            df.index = df.index.astype('datetime64[ns]')
            df = df.asfreq(freq='D')

            df.fillna(df["churn"].mean(), inplace=True)

            df = pd.concat([pd.Series(df.index), df["churn"].reset_index(drop=True)], ignore_index=True, axis=1)
            df.columns = ["ds", "y"]

            model = Prophet()
            model.fit(df)
            future = model.make_future_dataframe(periods=90, freq='D')
            forecast = model.predict(future)
            df_to_psql(company_Id=self.company_Id, table_name="churn_forecasting", df=forecast, primary_key="ds",
                       drop=True)
            wrangle_track(company_id=self.company_Id, method='Churn_Forecasting', message="Successfully completed")
            return {"CID": self.company_Id, "Status": "Completed"}

        except Exception as e:
            wrangle_track(company_id=self.company_Id, method='Churn_Forecasting', message=e)
            return {"CID": self.company_Id, "Status": e}

    def orders_forecasting(self):
        try:
            """
            Read sales_summary and count the order_id for each day 
            Predict the next 90 days forecasting using fb prophet
            store the cleaned file as order_forecasting
            """

            df_query = "SELECT * from sales_summary;"
            df = psql_to_df(self.company_Id, df_query)
            df["date_created"] = pd.to_datetime(df["date_created"]).dt.date
            max_date = max(df.date_created)
            new_max = max_date - datetime.timedelta(days=max_date.day)
            df = df[df['date_created'] <= new_max]
            grouper = df.groupby(["date_created"])["order_id"]
            df = pd.DataFrame(grouper.count())
            df.columns = ["orders"]

            df.index = df.index.astype('datetime64[ns]')
            df = df.asfreq(freq='D')

            df.fillna(df["orders"].mean(), inplace=True)

            df = pd.concat([pd.Series(df.index), df["orders"].reset_index(drop=True)], ignore_index=True, axis=1)
            df.columns = ["ds", "y"]

            # params = forecast_hyperparamter_tuning(df)
            # a = params["changepoint_prior_scale"]
            # b = params["holidays_prior_scale"]
            # c = params["n_changepoints"]
            # d = params["seasonality_mode"]
            #
            # holiday = pd.DataFrame([])
            # for date, name in sorted(holidays.UnitedStates(years=[2017, 2018, 2019, 2020]).items()):
            #     holiday = holiday.append(pd.DataFrame({'ds': date, 'holiday': "US-Holidays"}, index=[0]),
            #                              ignore_index=True)
            # holiday['ds'] = pd.to_datetime(holiday['ds'], format='%Y-%m-%d', errors='ignore')
            # holiday.head()
            #
            # model1 = Prophet(holidays=holiday, changepoint_prior_scale=a,
            #                 holidays_prior_scale=b,
            #                 n_changepoints=c,
            #                 seasonality_mode=d)
            model1 = Prophet()
            model1.fit(df)
            future = model1.make_future_dataframe(periods=90, freq='D')
            forecast = model1.predict(future)
            # forecast1=forecast.to_parquet("forecast.parquet")
            df_to_psql(company_Id=self.company_Id, table_name="order_forecasting", df=forecast,
                       primary_key="ds", drop=True)
            wrangle_track(company_id=self.company_Id, method='Order Forecasting', message="Successfully completed")
            return {"CID": self.company_Id, "Status": "Completed"}
        except Exception as e:
            wrangle_track(company_id=self.company_Id, method='Order Forecasting', message=e)
            return {"CID": self.company_Id, "Status": e}


def clean(company_Id):
    clean = Wrangle(company_Id)
    sales(company_Id=company_Id)
    clean.sales_executive()
    clean.sales_forecasting()
    clean.cust_segment()
    clean.cust_segment_one()
    clean.cohort_six()
    clean.cohort_year()
    clean.summ_rfm()
    clean.churn_forecasting()
    clean.orders_forecasting()


@p21_wrangle.get('/mag-wrangle', dependencies=[Depends(verify_token)])
async def magento_wrangle(interval):
    data = psql_to_df(Config.DB_NAME, "SELECT * FROM magento WHERE interval={};".format(interval))
    for index, row in data.iterrows():
        cid = row['company_id']
        clean(cid)
