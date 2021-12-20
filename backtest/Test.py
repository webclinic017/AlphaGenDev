#%%

#%%
# Use this file to check that cerebro has loaded the data
# Setup
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import backtrader as bt
import os
from datetime import datetime
from datetime import timedelta
from datetime import date
import yfinance as yf
import pandas as pd
import numpy as np
from tqdm import tqdm
import pyfolio as pf
from Strategies import *
from matplotlib import pyplot as plt
import pickle
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']

# Unpickle tickers
print("Unpickling Tickers")
with open(f'tickers', "rb") as input_file:
    tickers = pickle.load(input_file)

# When to start
initial_date = datetime(2010,1,1)
last_date = datetime(2020,1,1)
# Adds commissions
# 0.1% ... divide by 100 to remove the %

firstTime = True
if firstTime:
    cerebro = bt.Cerebro()
    print(f"Adding {len(tickers)} tickers")

    tickers = tqdm(tickers)
    not_added = 0
    message = ""
    for ticker in tickers:
        tickers.set_description(f"Adding {ticker} - {not_added} tickers failed to meet Cerebro. Last Error : {message} ")
        tickers.refresh() # to show immediately the update
        # Check if data is in memory
        try:
            if not os.path.isfile(f"{PATH_TO_SEC_DATA}/yahoo_finance/data/{ticker.lower()}.csv"):
                df = yf.Ticker(ticker).history(period="max")
                df.index = df.index.rename("DateTime")
                df = df[["Open", "High", "Low", "Close", "Volume"]]
                df = df.rename(columns = {"Open"   : "open",
                                        "High"   : "high",
                                        "Low"    : "low",
                                        "Close"  : "close",
                                        "Volume" : "volume"})
                df["datetime"] = pd.to_datetime(df.index, format = "%Y-%m-%d %H:%M:%S")
                # Keep only from initial_date
                df.to_csv(f"{PATH_TO_SEC_DATA}/yahoo_finance/data/{ticker.lower()}.csv", index = False)
            else:
                df = pd.read_csv(f"{PATH_TO_SEC_DATA}/yahoo_finance/data/{ticker.lower()}.csv")
                df.date = df.date.apply(lambda x : pd.to_datetime(x))
                df = df[(df.date >= initial_date) & (df.date <= last_date)]
                df.date = df.date.apply(lambda x: x + timedelta(seconds=0, minutes=0, hours=16))

            # idx = pd.date_range(initial_date, last_date)
            # df.index = df.date
            # df = df.reindex(idx, fill_value=np.nan)
            # df = df.fillna(method="ffill")
            # df.date = df.index
            # df = df.reset_index()
            # df.sort_values(by=['date'])
            # df.fillna(method="ffill")
            data = bt.feeds.PandasData(
                        dataname =  df,
                        datetime = 'date',
                        open     = 'open',
                        high     = 'high',
                        low      = 'low',
                        close    = 'close',
                        volume   = 'volume',
                        openinterest = None
                )
        

            cerebro.adddata(data, name = ticker)
        except Exception as e:
            message = e
            not_added = not_added + 1
 

    print("Pickling Cerebro")
    filename = f'{PATH_TO_SEC_DATA}/cerebros/cerebro_St'
    outfile = open(filename,'wb')
    pickle.dump(cerebro,outfile)
    outfile.close()


print("Unpickling Cerebro")
with open(f'cerebro_St', "rb") as input_file:
    cerebro = pickle.load(input_file)


cerebro.broker.setcash(1000000.0)
#cerebro.broker.setcommission(commission=0.001)

with open("{PATH_TO_SEC_DATA}/cerebros/cerebro_St", "rb") as f:
    cerebro = pickle.load(f)


# To analyze data
cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
#%%
# Add strategies
cerebro.addstrategy(St)
print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
results =cerebro.run()
print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

#strat = results[0]
#pyfoliozer = strat.analyzers.getbyname('pyfolio')
#returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
#output = pf.create_full_tear_sheet(returns)
# %%
