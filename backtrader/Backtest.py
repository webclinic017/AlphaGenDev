#%%

#%%
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
#%%
# Use this file to check that cerebro has loaded the data
# Setup


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
from AlphaGenStrategies import *
from matplotlib import pyplot as plt
import pickle
import time
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

firstTime = False
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
        #tickers.set_description(f"Adding {ticker} - {not_added} tickers failed to meet Cerebro. Last Error : {message} ")
        #tickers.refresh() # to show immediately the update
        # Check if data is in memory
        try:
            time.sleep(1)
            df = yf.download(ticker.lower(), initial_date.strftime("%Y-%m-%d"), last_date.strftime("%Y-%m-%d"))
            df = df_base.join(df)
            df = df.drop(columns=['any'])
            df = df.fillna(0.0)
            # We replace close values for the adj close values, that way we deal with returns at longer frequencies
            df['Close'] = df['Adj Close']
            data = bt.feeds.PandasData(dataname = df)
            cerebro.adddata(data = data, name = ticker)
        except Exception as e:
            message = e
            not_added = not_added + 1
 

    print("Pickling Cerebro")
    filename = f'{PATH_TO_SEC_DATA}/cerebros/cerebro_St'
    outfile = open(filename,'wb')
    pickle.dump(cerebro,outfile)
    outfile.close()

#%%


# Create versions of strategies
sts = [ {'nl': 50, 'ns':50, 'ts' : 'Eret', 'cp' : False},
 {'nl': 50, 'ns':50, 'ts' : 'Eret', 'cp' : True},
 {'nl': 50, 'ns':50, 'ts' : 'Fret', 'cp' : False},
 {'nl': 50, 'ns':50, 'ts' : 'Fret', 'cp' : True},
 {'nl': 50, 'ns':50, 'ts' : 'Eret_extended', 'cp' : False},
 {'nl': 50, 'ns':50, 'ts' : 'Eret_extended', 'cp' : True},
 {'nl': 50, 'ns':50, 'ts' : 'Fret_extended', 'cp' : False},
 {'nl': 50, 'ns':50, 'ts' : 'Fret_extended', 'cp' : True}]


for st in sts:
    print("Unpickling Cerebro")
    with open(f'{PATH_TO_SEC_DATA}/cerebros/cerebro_St', "rb") as input_file:
        cerebro = pickle.load(input_file)


    cerebro.broker.setcash(1000000.0)
    #cerebro.broker.setcommission(commission=0.001)

    # To analyze data
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')


    cerebro.addstrategy(St, type_signal = st['ts'], 
                        nl=st['nl'], 
                        ns=st['ns'], 
                        correct_precision = st['cp'])

    cp = 1 if st['cp']else 0
    name_st = f"{st['ts']}-{st['tnl']}-{st['ns']}-{cp}"

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    results =cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())


    strat = results[0]
    pyfoliozer = strat.analyzers.getbyname('pyfolio')

    returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
    transactions.to_csv(f'{PATH_TO_SEC_DATA}/cerebros/transactions_{name_st}')

    # Add the benchmark weights
    benchmark = pd.read_csv(f"{PATH_TO_SEC_DATA}\\cerebros\\benchmark_{name_st}.csv")

    benchmark.index = benchmark.Date.apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    benchmark.index = benchmark.index.tz_localize('UTC')
    benchmark = benchmark[['w_nasdaq', 'w_sp500', 'w_rusell']]
    # Add thetime series of returns for nasdaq sp500 and rusell
    df_b =yf.download(['^IXIC', '^GSPC', '^RUT'], initial_date.strftime("%Y-%m-%d"), last_date.strftime("%Y-%m-%d"))
    df_b.index = df_b.index.tz_localize('UTC')
    df_bm = df_b['Adj Close'].pct_change().join(benchmark)
    df_bm = df_bm.ffill()
    df_bm = df_bm.fillna(0.0)
    ben = df_bm['^GSPC']*df_bm['w_sp500']+df_bm['^IXIC']*df_bm['w_nasdaq']+df_bm['^RUT']*df_bm['w_rusell']

    ben.index = ben.index.rename('index')
    pf.create_full_tear_sheet(returns,  benchmark_rets =ben)
