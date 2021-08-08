
from pandas.core.frame import DataFrame
import requests
from requests.exceptions import HTTPError
import pandas as pd
import matplotlib.pyplot as plt
from pprint import pprint
from sqlalchemy import create_engine
from datetime import date
from datetime import datetime
from psycopg2 import connect, extensions, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

symbol = "ETHBUSD" ## BTCBUSD, ETHBUSD, LINKBUSD, LTCBTC

curr_datetime = datetime.now()
curr_time = curr_datetime.strftime("%H:%M:%S")

curr_time2 = datetime.now()
current_time = curr_time2.strftime("%H:%M:%S")
today = date.today()

now2 = datetime.now()
now2_1 = now2.strftime('%Y/%m/%d %H:%M:%S')


def get_data():

    try:
        response = requests.get("https://api.binance.com/api/v3/trades", 
                     params=dict(symbol=symbol))
        response.raise_for_status()
        jsonResponse = response.json()
        print("Entre JSON response")
        print(jsonResponse)
        print()
        print(jsonResponse[0].items())
        print()
        for i in jsonResponse:
            print(i)
        print()
        print("Pandas df below")
        print('-' * 45)
        df = pd.DataFrame(jsonResponse)
        df['time'] = pd.to_datetime(df['time'], unit='ms').apply(lambda x: x.to_datetime64())

        df.insert(1, 'symbol', symbol )
        print(df)

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

if __name__ == "__main__":
    get_data()