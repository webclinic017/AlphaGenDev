#--------------------------------------------------------
# PROGRAM NAME - YahooFinanceData.py 
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.1.0    [Mayor].[Minor].[Patch]
#
# PROJECT - AlphaGen 
#
# USAGE - Maintains Yahoo Finance
#
# REQUIRES - Python 3.9<=
#
# SYSTEM - All
#
# DATE - Apr 11 2021  (Sun) 
#
# BUGS - Not known
#	
#
# DESCRIPTION - Imports data from the yahoo finance
#			
#
# Log -  11/04/2021 Tries a parallel solution
#
#       07/05/2021 Weekly return directly in the construction e.g. 5 trading days before
#
#       11/05/2021 Optional parameter to download again all yahoo finance files
# 	
#--------------------p=E[mx]------------------------------

#%%
import pandas as pd 
import numpy as np
import datetime
from pandas.io.parsers import read_csv
import requests
import urllib
from tqdm.auto import tqdm
from tqdm import trange
from bs4 import BeautifulSoup
import os
import time
from multiprocessing import Process, Queue

#%%
import pandas as pd
import os
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']
df = pd.read_csv(f"{PATH_TO_SEC_DATA}/rebalance/2s.csv")
df.TICKER = df.TICKER.apply(lambda x : x.upper())
df.position_dollars = df.position_dollars.apply(lambda x : x/100)
df.to_csv(f"{PATH_TO_SEC_DATA}/rebalance/2sigma.csv", index = False)
#%%

def get_yahoo_finance_data(t, update_all=False):
    PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']
    PATH_TO_COMPUSTAT_CRSP_DATA=os.environ['PATH_TO_COMPUSTAT_CRSP_DATA']
    t=tqdm(t)
    # Not going before 1990 to avoid overloading
    before = int(time.mktime(datetime.date(1990,1,1).timetuple()))# Some starting date so I dont download all
    today=int(time.time())

    yahoo_finance=pd.DataFrame()
    
    not_found=0
    for ticker in t:
        url=f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={before}&period2={today}&interval=1d&events=history&includeAdjustedClose=true"
        try:
            if update_all or (not os.path.exists(os.path.join(PATH_TO_SEC_DATA, "yahoo_finance", "data", f"{ticker}.csv"))):
                time.sleep(1)
                t.set_description(f"Downloading {ticker} - {not_found}  not found ")
                t.refresh() # to show immediately the update
                temp_df=pd.read_csv(url)
                temp_df['tic']=ticker.upper()
                temp_df['ret']=[np.nan for i in temp_df['Adj Close']]
                try:
                    temp_df.loc[1:, ('ret')]=temp_df['Adj Close'][1:].values/temp_df['Adj Close'][:-1].values-1.0
                    #temp_df.loc[5:, ('retL5')]=temp_df['Adj Close'][5:].values/temp_df['Adj Close'][:-5].values-1.0
                except:
                    temp_df.loc[1:, ('ret')]=np.nan
                    #temp_df.loc[5:, ('retL5')]=np.nan

                temp_df['year']=[datetime.datetime.strptime(date, '%Y-%m-%d').year for date in temp_df.Date]
                temp_df['month']=[datetime.datetime.strptime(date, '%Y-%m-%d').month for date in temp_df.Date]
                temp_df['day']=[datetime.datetime.strptime(date, '%Y-%m-%d').day for date in temp_df.Date]

                # I want to make sure column names are 
                # date	open	high	low	close	adjclose	volume	tic	t_day	t_month	ret
                temp_df=temp_df.rename(columns={"Date": "date", "Open": "open", "High" : "high",
                                        "Low" : "low", "Close" : "close", "Adj Close" : "adjclose",
                                        "Volume" : "volume"})


                #yahoo_finance=yahoo_finance.append(temp_df)
                temp_df.to_csv(os.path.join(PATH_TO_SEC_DATA, "yahoo_finance", "data", f"{ticker}.csv"), index=False)
                t.set_description(f"{ticker} Downloaded - {not_found}  not found ")
                t.refresh()
            else:
                t.set_description(f"{ticker}.csv already exists- {not_found}  not found ")
                t.refresh() # to show immediately the update

        except: 
            not_found+=1
            t.set_description(f"{ticker} not processed - {not_found} in total")
            t.refresh() # to show immediately the update
            continue

    #yahoo_finance.to_csv(os.path.join(PATH_TO_SEC_DATA, "yahoo_finance",f"yahoo_finance{id}.csv"), index=False)

#%% Appends yahoo finance data





#%%
if __name__=='__main__':
    PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']
    PATH_TO_COMPUSTAT_CRSP_DATA=os.environ['PATH_TO_COMPUSTAT_CRSP_DATA']

    start=time.time()
    df=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "cik_ticker.csv"))

    tickers=df.ticker #[1:10]

    # For etfs
    #tickers=["XLB", "XLI", "XLY", "XLP", "XLE", "XLF", "XLU", "XLV", "XLK"]

    print(len(tickers))
    get_yahoo_finance_data(tickers, update_all=True)
    #nb= 1 #os.cpu_count() The API breaks if we try more
    # batches=np.array_split(tickers,nb)
    # print(f"Batches of size {[len(batch) for batch in batches]}")

    # print(f"Spliting the sample into {len(batches)} batches")
    # processes = [Process(target=get_yahoo_finance_data, args=(batches[i], i)) for i in [0]]#range(nb)]

    # for p in processes:
    #     p.start()

    # for p in processes:
    #     p.join()

    # end=time.time()
    # time_ellapsed=end-start
    # t=round(time_ellapsed, 2)
    # n=len(tickers)
    # print(f"{t} seconds - for {n} tickers")



