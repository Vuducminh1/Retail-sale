import datetime
import sqlalchemy as db
import pandas as pd
from enum import Enum
from etl import conn, engine


engine_str = (
      "mysql+pymysql://{user}:{password}@{server}:{port}/{database}".format(
       user      =  "root",
       password  =  "123",
       server    =  "localhost",
       port      =  "3306",
       database  =  "household_olap"))
engine_2 = db.create_engine(engine_str)
conn_2 = engine_2.connect()


def convert_time(week_numbers):
    dates = pd.to_datetime(week_numbers * 7, origin='2023-01-01', unit='D')
    months = pd.Series(dates.dt.month, name='month')
    quarters = pd.Series(dates.dt.quarter, name='quarter')
    years = pd.Series((week_numbers-1) // 52 + 1, name='year')
    return pd.concat([week_numbers, months, quarters, years], axis=1)

if __name__ == '__main__':
    if (conn):
        if (conn_2):
            print("MySQL Connection is Successful ... ... ...")
            sql = "SELECT DISTINCT week_no FROM transaction"
            week_numbers = pd.read_sql_query(db.text(sql), conn).sort_values(by=['week_no'])
            df = convert_time(week_numbers['week_no'])
            df.to_sql(name="dim_time", con=engine_2,
                schema="household_olap",
                if_exists = "append", chunksize = 1000,
                index=False)

    else:
        print("MySQL Connection is not Successful ... ... ...")

    conn.close()

    # week_numbers = pd.Series([6, 10, 25, 40, 53], name='week_numbers')
    # print(convert_time(week_numbers))