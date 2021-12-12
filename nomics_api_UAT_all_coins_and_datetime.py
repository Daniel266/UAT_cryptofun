

'''
HELPFULL LINKS:
https://pynative.com/python-sqlite/#h-python-sqlite-database-connection
https://coralogix.com/blog/python-logging-best-practices-tips/

'''

from os import error
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
import json
from pandas import json_normalize
import urllib.request
import sys, time, os
from pathlib import Path
import sqlite3
import logging

'''
ISSUES:
1. Copying all previous data rows and adding current iteration
- of three data rows which causes duplicates
UPDATE: this issue has been resolved by modifying the following line:

sql_data = tickers.to_sql(sqlite_table, sqlite_connection, if_exists='append')
to:
sql_data = tickers.to_sql(sqlite_table, sqlite_connection, if_exists='replace') 
(12-11-21)

AGENDA:
1. Create SQLite database to store data incrementally (all data, not just new data)
 -- will require append
2. Add logging

'''

logging.basicConfig(filename=r'C:\Users\danau\Documents\Python\Projects\otherp\test-app\env\nomics_api_UAT_all_coins_and_datetime.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.DEBUG)

now = datetime.now()
today = date.today()

current_time = now.strftime("%H:%M:%S")
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

logging.info("Formatting dates {today} and times {now}, {current_time}, {dt_string}".format(today = today, now = now, current_time = current_time, dt_string = dt_string))

def info():
    
    NOMICS_API_KEY= "e86ad175151309bad9942b95bb0d7a002d64cad8"

    url_market = "https://api.nomics.com/v1/markets?key={}".format(NOMICS_API_KEY)
    url_prices = "https://api.nomics.com/v1/prices?key={}".format(NOMICS_API_KEY)
    url_ticker = "https://api.nomics.com/v1/currencies/ticker?key="+(NOMICS_API_KEY)+"&BTC,ETH,XRP,ADA,LTC,LRC,GALA,LINK,CRO,DOT,ATOM,AMP,ALGOA,MANA,FIL,OXT,DNT&interval=1d,30d&convert=USD&per-page=100&page=1"
    url_currency = "https://api.nomics.com/v1/currencies?key="+(NOMICS_API_KEY)+"&ids=BTC,ETH,XRP,ADA,LTC,LRC,GALA,LINK,CRO,DOT,ATOM,AMP,ALGOA,MANA,FIL,OXT,DNT&attributes=id,name,logo_url"

    logging.info("Retrieving NOMICS_API_KEY: {NOMICS_API_KEY}")
    logging.info("url_market = {url_market}")
    logging.info("url_prices = {url_prices}")
    logging.info("url_ticker = {url_ticker}")
    logging.info("url_currency = {url_currency}")

    return url_market, url_currency, url_prices, url_ticker, NOMICS_API_KEY


url_market, url_currency, url_prices, url_ticker, NOMICS_API_KEY = info()

def get_tickers():
    r = requests.get(url_ticker)
    results = r.json()
    FIELDS = ["1d.volume", "1d.volume_change", "1d.volume_change_pct", "1d.price_change",\
        "1d.price_change_pct", "30d.volume", "30d.volume_change"]

    tickers = pd.json_normalize(results)
    tickers.insert(39,'inserted_date', today)
    # tickers['inserted_date'] = pd.to_datetime(tickers['inserted_date'])
    tickers.insert(40,'inserted_time', current_time)
    # tickers['inserted_time'] = pd.to_datetime(tickers['inserted_time'])
    tickers.insert(41,'inserted_datetime',dt_string)
    # tickers['inserted_datetime'] = pd.to_datetime(tickers['inserted_datetime'])

    print(tickers)
    return tickers

def to_sql():
    engine = create_engine('sqlite:///nomics_test_uat_all_coins_datetime.db', echo=True)
    sqlite_connection = engine.connect()
    try:
        # sql = '''
        #         DROP TABLE nomics
        #         '''
        sqlite_table = "nomics_test_uat_all_coins_datetime"
    except ValueError as e:
        # INSERT INTO nomics
        # CREATE TABLE IF NOT EXISTS
        # DROP TABLE
        sql = ''' 
                INSERT INTO nomics_test_uat_all_coins_datetime(id,
                                   platform_currency,
                                   symbol,
                                   name,
                                   status,
                                   price,
                                   price_date,
                                   price_timestamp,
                                   circulating_supply,
                                   max_supply,
                                   market_cap,
                                   market_cap_dominance,
                                   num_exchanges,
                                   num_pairs,
                                   num_pairs_unmapped,
                                   first_candle,
                                   first_trade,
                                   first_order_book,
                                   first_priced_at,
                                   rank,
                                   rank_delta,
                                   high,
                                   high_timestamp,
                                   1d.price_change,
                                   1d.price_change_pct,
                                   1d.volume_change,
                                   1d.volume_change_pct,
                                   1d.market_cap_change,
                                   1d.market_cap_change_pct,
                                   30d.volume,
                                   30d.price_change,
                                   30d.price_change_pct,
                                   30d.volume_change,
                                   30d.volume_change_pct,
                                   30d.market_cap_change,
                                   30d.market_cap_change_pct,
                                   inserted_date,
                                   inserted_time,
                                   inserted_datetime)
                                   VALUES(?,?,?,?)
                    '''
    sql_data = tickers.to_sql(sqlite_table, sqlite_connection, if_exists='replace') # fail, replace, append

    return engine, sqlite_connection, sqlite_table, sql_data

def copy_sql():
    # engine_copy_sql = create_engine('sqlite:///nomics_test_uat_all_coins_datetime.db', echo=True)
    # sqlite_connection_copy_sql = sqlite.connect()
    try:
        sqliteConnection = sqlite3.connect('nomics_test_uat_all_coins_datetime.db')
        cursor = sqliteConnection.cursor()
        print("Database created and Successfully Connected to SQLite")

        sql_insert = '''
                    INSERT INTO nomics_test_uat_all_coins_datetime_full_table
                    SELECT * FROM nomics_test_uat_all_coins_datetime
                     '''
        cursor.execute(sql_insert)
        # sqlite_select_Query = "select sqlite_version();"
        # cursor.execute(sqlite_select_Query)
        record = cursor.fetchall()
        print("SQLite Database Version is: ", record)
        cursor.close()
        
    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")
        # try:
        #     sqlite_table_insert = "nomics_test_uat_all_coins_datetime_full_table"
        # except


    # cursor = sqlite_connection.cursor()
    # cursor.execute(sql_insert)

    # cursor.close()

    # sql_data_insert = 

def from_sql():
    try:
        ''' df below is causing issues due to copying entire data table from
            nomics_test_uat
            Have to get and copy only new data as a result of each iteration
            '''
        df = pd.read_sql('SELECT * FROM nomics_test_uat_all_coins_datetime', sqlite_connection)
        df.to_csv('nomics', index=False)
        # df_uat = 
    except ValueError as e:
        sql = '''
               INSERT INTO nomics_test_uat_all_coins_datetime(id,
                                   platform_currency,
                                   symbol,
                                   name,
                                   status,
                                   price,
                                   price_date,
                                   price_timestamp,
                                   circulating_supply,
                                   max_supply,
                                   market_cap,
                                   market_cap_dominance,
                                   num_exchanges,
                                   num_pairs,
                                   num_pairs_unmapped,
                                   first_candle,
                                   first_trade,
                                   first_order_book,
                                   first_priced_at,
                                   rank,
                                   rank_delta,
                                   high,
                                   high_timestamp,
                                   1d.price_change,
                                   1d.price_change_pct,
                                   1d.volume_change,
                                   1d.volume_change_pct,
                                   1d.market_cap_change,
                                   1d.market_cap_change_pct,
                                   30d.volume,
                                   30d.price_change,
                                   30d.price_change_pct,
                                   30d.volume_change,
                                   30d.volume_change_pct,
                                   30d.market_cap_change,
                                   30d.market_cap_change_pct,
                                   inserted_date,
                                   inserted_time,
                                   inserted_datetime)
                                   VALUES(?,?,?,?)
                    '''

    df = DataFrame(df)
    sqlite_connection.close()

    # print(df)
    # print('--- Dataframe info below ---')
    # print(df.info())
    return df

def to_postgres():

    conn = connect(dbname='postgres', user='postgres', host='localhost', password='a')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    sql = '''CREATE database nomics_test_uat_all_coins_datetime'''
    try:
        # cur.execute('CREATE DATABASE nomics_test_UAT')
        cur.execute(sql)
    except (psycopg2.Error):
        pass

    engine = create_engine('postgresql://postgres:a@localhost:5432/nomics_test_uat_all_coins_datetime')
    try:
        df.to_sql('nomics_test_uat_all_coins_datetime', engine, if_exists='append') # append, drop, replace, fail
    except ValueError:
        df.to_sql('nomics_test_uat_all_coins_datetime2', engine, if_exists='append') # append



    cur.close()
    conn.close()

# def from_postgres():

#     filePath = r'C:\Users\danau\Documents\Python\Projects\otherp\test-app\env\nomics.txt'
#     if os.path.exists(filePath):
#         try:
#             os.remove(filePath)
#         except error as e:
#             print(e)

#     # open(r'C:\Users\danau\Documents\Python\Projects\otherp\test-app\env\nomics.txt', 'w')
#     s = '''
#             SELECT * FROM [PostgreSQL 13].nomics_test.nomics_table_test2
#         '''
    
#     conn = psycopg2.connect(dbname='postgres', user='postgres', host='localhost', password='a')
#     db_cursor = conn.cursor()
#     sql = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(s)
#     with open(filePath, 'w') as f_output:
#         db_cursor.copy_expert(sql, f_output)

#     conn.close()

def from_postgres():

    db_conn = connect(dbname='nomics_test_uat_all_coins_datetime', user='postgres', host='localhost', password='a')
    db_cursor = db_conn.cursor()


    # sql = db_cursor.execute(""" SELECT * FROM "nomics_table_test2" """)

    # sql = "COPY (SELECT * FROM nomics_table_test2) TO STDOUT WITH CSV DELIMITER ';' "

    sql = "COPY (SELECT * FROM nomics_test_uat_all_coins_datetime) TO STDOUT WITH CSV HEADER"

    # sql = '''
    #         SELECT
    #             id,
    #             currency,
    #             symbol,
    #             name,
    #             status,
    #             price,
    #             price_date,
    #             price_timestamp,
    #             circulating_supply,
    #             max_supply,
    #             market_cap,
    #             market_cap_dominance,
    #             num_exchanges,
    #             num_pairs,
    #             num_pairs_unmapped,
    #             first_candle,
    #             first_trade,
    #             first_order_book,
    #             rank
    #             rank_delta,
    #             high,
    #             high_timestamp,
    #             1d.price_change,
    #             1d.price_change_pct,
    #             1d.volume_change,
    #             1d.volume_change_pct,
    #             1d.market_cap_change,
    #             1d.market_cap_change_pct,
    #             30d.volume,
    #             30d.price_change,
    #             30d.price_change_pct,
    #             30d.volume_change,
    #             30d.volume_change_pct,
    #             30d.market_cap_change,
    #             30d.market_cap_change_pct    
    #         FROM nomics_test.nomics_table_test2
    #       '''

    # SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(sql)



    filePath = r'C:\Users\danau\Documents\Python\Projects\otherp\test-app\env\nomics.txt'

    with open(filePath, "w") as file:
        db_cursor.copy_expert(sql, file)

        file.close()

    # try:
    #     with open(filePath, 'w') as f_output:
    #      db_cursor.copy_expert(SQL_for_file_output, f_output)
    # except psycopg2.Error as e:
    #     print(e)

        ## t_message = "Error: " + e + "/n query we ran: " + str(sql) + "/n filePath: " + filePath

    db_cursor.close()
    db_conn.close()




def to_txt():
    txtFile = Path(r'C:\Users\danau\Documents\Python\Projects\otherp\test-app\env\nomics.txt')
    # df.to_csv(r'C:\Users\danau\Documents\Python\Projects\otherp\test-app\env\nomics.txt', sep='\t')
    # txtFile = str(txtFile)
    # if txtFile.isfile():
    if txtFile.exists():
        try:
            os.remove(txtFile)
            df.to_csv(r'C:\Users\danau\Documents\Python\Projects\otherp\test-app\env\nomics.txt', sep='\t')
            # with open(txtFile, "a") as file_object:
            #     file_object.write(df.to_csv(r'C:\Users\danau\Documents\Python\Projects\otherp\test-app\env\nomics.txt', sep='\t'))
            #     file_object.close()
        except error as e:
            # print(e)
            pass
    

## Calls in UAT:
# tickers = get_tickers()
# engine, sqlite_connection, sqlite_table, sql_data = to_sql()
# df = from_sql()
# to_postgres()

## Calls in prod:
tickers = get_tickers()
engine, sqlite_connection, sqlite_table, sql_data = to_sql()
# copy_sql()
df = from_sql()
to_postgres()
from_postgres()
to_txt()
