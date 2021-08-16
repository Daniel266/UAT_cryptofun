


'''
https://tiao.io/post/exploring-the-binance-cryptocurrency-exchange-api-orderbook/
'''


from pandas.core.frame import DataFrame
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pprint import pprint
from sqlalchemy import create_engine
from datetime import date
from datetime import datetime
import psycopg2
from psycopg2 import connect, extensions, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# symbol = "ETHBUSD" ## BTCBUSD, ETHBUSD, LINKBUSD, LTCBTC
symbol = ['ETHBUSD', 'BTCBUSD', 'LINKBUSD', 'LTCBTC']

curr_datetime = datetime.now()
curr_time = curr_datetime.strftime("%H:%M:%S")

curr_time2 = datetime.now()
current_time = curr_time2.strftime("%H:%M:%S")
today = date.today()

now2 = datetime.now()
now2_1 = now2.strftime('%Y/%m/%d %H:%M:%S')


def get_data():

    data = []
    symbols = []

    for i in symbol:

        sym = i
        # print(sym)

        r = requests.get("https://api.binance.com/api/v3/trades", ## {{url}}/api/v3/ticker/price
                 params=dict(symbol=i)) ## ETHBUSD
        results = r.json()

        ''' Append working!!! '''
        y = {"symbol": i}
        for row in results:
            data.append(row)
            symbols.append(y)
    df = pd.DataFrame(data)
    df_sym = pd.DataFrame(symbols)
    df["symbol"] = pd.Series(df_sym["symbol"])
    print(df)
    return df # data, df


def to_sql():
    engine = create_engine('sqlite:///binances3_test.db', echo=True)
    sqlite_connection = engine.connect()
    try:
        sqlite_table = "Binance33"
    except ValueError as e:
        sql = '''
                INSERT INTO Binance(id,
                                    symbol
                                    price,
                                    qty,
                                    quoteQty,
                                    time,
                                    isBuyerMaker,
                                    isBestMatch,
                                    symbol)
                                    VALUES(?,?,?,?)'''
    sql_data = data.to_sql(sqlite_table, sqlite_connection, if_exists='append') ## if_exists='fail','append'

    return engine, sqlite_connection, sqlite_table, sql_data

def from_sql():
    try:
        df = pd.read_sql('SELECT * FROM Binance33', sqlite_connection)
        df.to_csv('Binance3', index=False)
    except ValueError as e:
        sql = '''
                INSERT INTO Binance(id,
                                    symbol
                                    price,
                                    qty,
                                    quoteQty,
                                    time,
                                    isBuyerMaker,
                                    isBestMatch,
                                    symbol)
                                    VALUES(?,?,?,?)'''
    df = DataFrame(df)
    sqlite_connection.close()
    
    print(df)
    return(df)

def to_postgres():

    conn = connect(dbname='postgres', user='postgres', host='localhost', password='a')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    try:
        cur.execute('CREATE DATABASE binanceee3')
    except (psycopg2.Error):
        pass
    
    engine = create_engine('postgresql://postgres:a@localhost:5432/binanceee3')
    try:
        df.to_sql('table_main1', engine)
    except ValueError:
        df.to_sql('table_main3', engine, if_exists='append') ## table_main2 table_main3

    cur.close()

    conn.close()

data = get_data()
engine, sqlite_connection, sqlite_table, sql_data = to_sql() # tuple
print(engine)
df = from_sql()
to_postgres()