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
with open(f'{PATH_TO_SEC_DATA}/cerebros/tickers', "rb") as input_file:
    tickers = pickle.load(input_file)

# When to start
initial_date = datetime(2010,1,1)
last_date = datetime.today()
# Adds commissions
# 0.1% ... divide by 100 to remove the %

firstTime = True
if firstTime:
    cerebro = bt.Cerebro()
    print(f"Adding {len(tickers)} tickers")

    tickers = tqdm(tickers)
    not_added = 0
    message = ""

    # Use AAPL as base ticker so that the backtest has the entire span
    df_base = yf.download('AAPL', initial_date.strftime("%Y-%m-%d"), last_date.strftime("%Y-%m-%d"))
    df_base["any"] = 0.0
    df_base = df_base[["any"]]

    for ticker in tickers:
        tickers.set_description(f"Adding {ticker} - {not_added} tickers failed to meet Cerebro. Last Error : {message} ")
        tickers.refresh() # to show immediately the update
        # Check if data is in memory
        try:
            df = yf.download(ticker.lower(), initial_date.strftime("%Y-%m-%d"), last_date.strftime("%Y-%m-%d"))
            df = df_base.join(df)
            df = df.drop(columns=['any'])
            df = df.fillna(0.0)
            cerebro.adddata(df, name = ticker)
        except Exception as e:
            message = e
            not_added = not_added + 1
 

    print("Pickling Cerebro")
    filename = f'{PATH_TO_SEC_DATA}/cerebros/cerebro_St'
    outfile = open(filename,'wb')
    pickle.dump(cerebro,outfile)
    outfile.close()


print("Unpickling Cerebro")
with open(f'{PATH_TO_SEC_DATA}/cerebros/cerebro_St', "rb") as input_file:
    cerebro = pickle.load(input_file)


cerebro.broker.setcash(1000000.0)
#cerebro.broker.setcommission(commission=0.001)



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
