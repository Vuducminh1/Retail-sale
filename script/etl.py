import sqlalchemy as db
import pandas as pd
from datetime import datetime
from enum import Enum

class Env(Enum):
    ROOT_FOLDER = '../../data'
    SCHEMA_PATH = '../../oltp/schema.sql'

engine_str = (
      "mysql+pymysql://{user}:{password}@{server}:{port}/{database}".format(
       user      =  "root",
       password  =  "123",
       server    =  "localhost",
       port      =  "3306",
       database  =  "household"))
engine = db.create_engine(engine_str)
conn = engine.connect()

def delelte_all_tables(engine):
    meta = db.MetaData()
    meta.reflect(bind=engine)
    meta.drop_all(bind=engine)

def create_tables_from_schema(conn):
    with open(Env.SCHEMA_PATH.value) as file:
        sql_command = ''
        for line in file:
        # Ignore commented lines
            if not line.startswith('--') and line.strip('\n'):
                # Append line to the command string
                sql_command += line.strip('\n')

                # If the command string ends with ';', it is a full statement
                if sql_command.endswith(';'):
                    # Try to execute statement and commit it
                    try:
                        conn.execute(db.text(sql_command))
                        conn.commit()
                    # Assert in case of error
                    except:
                        print('Ops')

                    # Finally, clear command string
                    finally:
                        sql_command = ''



def etl_hh_demographic():
    df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/hh_demographic.csv')
    print(df.head())
    df['MARITAL_STATUS_CODE'] = df['MARITAL_STATUS_CODE'].map({
                                                        'A': 'Married',
                                                        'B': 'Single',
                                                        'U': 'Unknown'
                                                    })
    print()
    print()
    print(df.head())
    
    # df.to_sql(name="hh_demographic", con=engine,
    #             schema="household",
    #             if_exists = "append", chunksize = 1000,
    #             index=False)
    

def etl_campaign_desc():
    df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/campaign_desc.csv')
    df = df.rename(columns={'CAMPAIGN': 'CAMPAIGN_ID'})
    # df.to_sql(name="campaign_desc", con=engine,
    #             schema="household",
    #             if_exists = "append", chunksize = 1000,
    #             index=False)


def etl_campaign():
    df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/campaign_table.csv')
    df = df.rename(columns={'CAMPAIGN': 'CAMPAIGN_ID'})
    hh_df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/hh_demographic.csv')
    mask = df['household_key'].isin(hh_df['household_key'])
    df = df[mask]
    # df.to_sql(name="campaign", con=engine,
    #             schema="household",
    #             if_exists = "append", chunksize = 1000,
    #             index=False)
    

def etl_product():
    df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/product.csv')
    df = df.rename(columns={'MANUFACTURER': 'MANUFACTURER_ID'})
    # df.to_sql(name="product", con=engine,
    #             schema="household",
    #             if_exists = "append", chunksize = 1000,
    #             index=False)
    

def etl_causal():
    df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/causal_data.csv')
    df['display'] = df['display'].map({
                                    '0': 'Not on Display',
                                    '1': 'Store Front',
                                    '2': 'Store Rear',
                                    '3': 'Front End Cap',
                                    '4': 'Mid-Aisle End Cap',
                                    '5': 'Rear End Cap',
                                    '6': 'Side-Aisle End Cap',
                                    '7': 'In-Aisle',
                                    '9': 'Sencondary Location Display',
                                    'A': 'In-Shelf'
                                })
    df['mailer'] = df['mailer'].map({
                                    '0': 'Not on ad',
                                    'A': 'Interior page feature',
                                    'C': 'Interior page line item',
                                    'D': 'Front page feature',
                                    'F': 'Back page feature',
                                    'H': 'Wrap front feature',
                                    'J': 'Wrap interior coupon',
                                    'L': 'Wrap back feature',
                                    'P': 'Interior page coupon',
                                    'X': 'Free on interior page',
                                    'Z': 'Free on front page, back page or wrap'
                                })
    df.to_sql(name="causal", con=engine,
                schema="household",
                if_exists = "append", chunksize = 1000,
                index=False)
    

def etl_coupon():
    df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/coupon.csv')
    df = df.rename(columns={'CAMPAIGN': 'CAMPAIGN_ID'})
    # df.to_sql(name="coupon", con=engine,
    #             schema="household",
    #             if_exists = "append", chunksize = 1000,
    #             index=False)
    
    
def etl_coupon_redempt():
    df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/coupon_redempt.csv')
    df = df.rename(columns={'CAMPAIGN': 'CAMPAIGN_ID'})
    hh_df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/hh_demographic.csv')
    mask = df['household_key'].isin(hh_df['household_key'])
    df = df[mask]
    # df.to_sql(name="coupon_redempt", con=engine,
    #             schema="household",
    #             if_exists = "append", chunksize = 1000,
    #             index=False)
    
def etl_transaction():
    df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/transaction_data.csv', dtype={'TRANS_TIME': str})
    hh_df = pd.read_csv(f'{Env.ROOT_FOLDER.value}/hh_demographic.csv')
    mask = df['household_key'].isin(hh_df['household_key'])
    df = df[mask]
    df['TRANS_TIME'] = df['TRANS_TIME'].apply(lambda x: list(str(x)))   \
                                        .apply(lambda x: datetime.strptime(''.join(x[:2] + [':'] + x[2:]), '%H:%M').time())
    # df.to_sql(name="transaction", con=engine,
    #             schema="household",
    #             if_exists = "append", chunksize = 1000,
    #             index=False)



if __name__ == "__main__":
    if (conn):
        print("MySQL Connection is Successful ... ... ...")
        print()
        print()
        # delelte_all_tables(engine=engine)
        create_tables_from_schema(conn=conn)
        # etl_hh_demographic()
        # etl_campaign_desc()
        # etl_campaign()
        # etl_product()
        etl_causal()
        # etl_coupon()
        # etl_coupon_redempt()
        # etl_transaction()

        # query="SELECT * FROM hh_demographic"
        # my_data=list(conn.execute(db.text(query)))
        # print(my_data)
    else:
        print("MySQL Connection is not Successful ... ... ...")

    # dataset.to_sql(name="irisdata", con=engine,
    #                 schema="datasciencerecipes",
    #                 if_exists = "replace", chunksize = 1000,
    #                 index=False)
    conn.close()