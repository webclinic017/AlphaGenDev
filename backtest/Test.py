#%%
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
initial_date = datetime(2011,1,1)
last_date = datetime(2021,9,1)
# Adds commissions
# 0.1% ... divide by 100 to remove the %

# Unpickle
#%%
load_again = False
#tickers = ['aapl']
if load_again:
    initial_date_   = initial_date.strftime("%Y-%m-%d")
    last_date_      = last_date.strftime("%Y-%m-%d")
    tickers = tqdm(tickers)
    not_added = 0
    message = ""
    cerebro = bt.Cerebro()

    # We use APPL as the time benchmark, we need complete dataframes even if they have 0s
    df = yf.download('aapl', initial_date_, last_date_)
    temp_df = df[['Open']]
    temp_df.Open = 0.0
    temp_df = temp_df.rename(columns = {'Open' : 'temp'})
   
    for ticker in tickers:
        tickers.set_description(f"Adding {ticker} - {not_added} tickers failed to meet Cerebro. Last Error : {message} ")
        tickers.refresh() # to show immediately the update
        try:
            df = yf.download(ticker.upper(), initial_date_, last_date_)
            if len(df):

                df= temp_df.join(df)
                df = df.drop(['temp'], axis=1)
                df = df.fillna(0)
                data = bt.feeds.PandasData(dataname=df)
                cerebro.adddata(data, name = ticker)
            else:
                not_added = not_added + 1
        except Exception as e:
            message = e
            not_added = not_added + 1

    #%%

    print("Pickling Cerebro")
    filename = f'cerebro_St'
    outfile = open(filename,'wb')
    pickle.dump(cerebro,outfile)
    outfile.close()

#%%
#cerebro.broker.setcommission(commission=0.001)
with open("cerebro_St", "rb") as f:
    cerebro = pickle.load(f)

cerebro.addstrategy(St)
cerebro.broker.setcash(1000000.0)
# To analyze data
cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')


#%%
print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
results =cerebro.run()
print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())


# %%
