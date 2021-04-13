import pandas as pd
import datetime
from datetime import datetime, tzinfo, timedelta  ## pip install datetime
from dateutil import tz

PST = tz.tzoffset("PST", -28800)  # datetime.tzinfo.FixedOffset(-(8*60),"PST")
UTC = tz.tzutc()


def strTimeSt(PSTConvert=False):
    """return a string representation of a datetime object. Uses UTC by default
  Can also convert to PST with PSTConvert=True. Never converts to PDT."""
    t = datetime.now(tz=UTC)
    tf = t if not PSTConvert else t.astimezone(tz=PST)

    return tf.strftime("%Y-%m-%d %H:%M:%S %Z (%z)")


def scheduler_track(company_id, method, message):
    try:
        df = pd.read_csv('scheduler_logs.csv', index_col="Unnamed: 0")
    except FileNotFoundError:
        df = pd.DataFrame()

    data = {"company_id": company_id,
            "Function": method,
            "Message": message,
            "Time": strTimeSt()}

    df_data = pd.DataFrame.from_dict([data])
    df = df.append(df_data, ignore_index=True, sort=False)

    df.to_csv('scheduler_logs.csv')


def wrangle_track(company_id, method, message):
    try:
        df = pd.read_csv('wrangle_logs.csv', index_col="Unnamed: 0")
    except FileNotFoundError:
        df = pd.DataFrame()

    data = {"company_id": company_id,
            "Function": method,
            "Message": message,
            "Time": strTimeSt()}

    df_data = pd.DataFrame.from_dict([data])
    df = df.append(df_data, ignore_index=True, sort=False)

    df.to_csv('wrangle_logs.csv')


def download_csv_track(company_id, name):
    try:
        df = pd.read_csv('download_csv_logs.csv', index_col="Unnamed: 0")
    except FileNotFoundError:
        df = pd.DataFrame()

    data = {"company_id": company_id,
            "File Name": name,
            "Time": strTimeSt()}

    df_data = pd.DataFrame.from_dict([data])
    df = df.append(df_data, ignore_index=True, sort=False)

    df.to_csv('download_csv_logs.csv')


def mailchimp_track(company_id, length):
    try:
        df = pd.read_csv('mailchimp_logs.csv', index_col="Unnamed: 0")
    except FileNotFoundError:
        df = pd.DataFrame()

    data = {"company_id": company_id,
            "Number of emails exported": length,
            "Time": strTimeSt()}

    df_data = pd.DataFrame.from_dict([data])
    df = df.append(df_data, ignore_index=True, sort=False)

    df.to_csv('mailchimp_logs.csv')


def import_track(company_id, method, message):
    try:
        df = pd.read_csv('initial_import_logs.csv', index_col="Unnamed: 0")
    except FileNotFoundError:
        df = pd.DataFrame()

    data = {"company_id": company_id,
            "Function": method,
            "Message": message,
            "Time": strTimeSt()}

    df_data = pd.DataFrame.from_dict([data])
    df = df.append(df_data, ignore_index=True, sort=False)

    df.to_csv('initial_import_logs.csv')


def insert_logs(company_id, method, message):
    try:
        df = pd.read_csv('insert_logs.csv', index_col="Unnamed: 0")
    except FileNotFoundError:
        df = pd.DataFrame()

    data = {"company_id": company_id,
            "Function": method,
            "Message": message,
            "Time": strTimeSt()}

    df_data = pd.DataFrame.from_dict([data])
    df = df.append(df_data, ignore_index=True, sort=False)

    df.to_csv('insert_logs.csv')
