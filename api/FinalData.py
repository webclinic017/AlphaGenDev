#--------------------------------------------------------
# PROGRAM NAME - FinalData.py 
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.0.1    [Mayor].[Minor].[Patch]
#
# PROJECT - AlphaGen 
#
# USAGE - Computes the final data used in the signal and backtest computation
#
# REQUIRES - Python 3.9<=
#
# SYSTEM - All
#
# DATE - Apr 13 2021  (Tue) 
#
# BUGS - Not known
#	
#
# DESCRIPTION - Joins the data from yahoo finance with the data from sec and compustat to have a single one 
#			
#
# Log -  
# 	
#--------------------p=E[mx]------------------------------

#%%
import pandas as pd 
import numpy as np
from datetime import datetime, timedelta, date
import requests
import urllib
from tqdm.auto import tqdm
from tqdm import trange
from bs4 import BeautifulSoup
import os
import time
from multiprocessing import Process, Queue
import matplotlib.pyplot as plt

#%% Loads basic files
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']
PATH_TO_COMPUSTAT_CRSP_DATA=os.environ['PATH_TO_COMPUSTAT_CRSP_DATA']

#%% Appends yahoo finance data
# To avoid unnecessary data loading,
def append_yahoo_finance(tickers, id):
    
    df_yahoo=pd.DataFrame()

    for ticker in tqdm(tickers):
        path_file=os.path.join(PATH_TO_SEC_DATA, 'yahoo_finance','data', f'{ticker}.csv')
        if os.path.exists(path_file):
            temp=pd.read_csv(path_file)
            df_yahoo=df_yahoo.append(temp)

    df_yahoo=df_yahoo.rename(columns={'date' : 't_day'})
    df_yahoo=df_yahoo.rename(columns={'tic' : 'ticker'})
    def std_ticker(x):
        try:
            return x.lower()
        except:
            return x
    df_yahoo.ticker=df_yahoo.ticker.apply(std_ticker)
    #return df_yahoo
    df_yahoo.to_csv(os.path.join(PATH_TO_SEC_DATA, 'yahoo_finance', f"aggregated_yf{id}.csv"), index=False)



#%%
def merge_yf_sec():
# We start with the pricing data and then add the accouting data
    df_yahoo=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, 'yahoo_finance', 'aggregated_yf.csv'))

    df_sec=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "fundamentals.csv"))
    df_sec=df_sec.sort_values(['cik', 't_day'])
    df_link=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "cik_ticker.csv"))
    df=df_yahoo.merge(df_link, on=['ticker']) #31414117
    df=df.dropna() #31089236
    df=df.sort_values(['cik', 't_day'])

    df=df.merge(df_sec, how='left',on=['cik', 't_day'])

    to_ffill=['atq', 'cheq', 'cshoq', 'oiadpq', 'seqq']

    for var in to_ffill:
        df[var] = df.groupby(['cik'])[var].ffill()

    # we need the sec or compustat data
    

    etfs=["IYW", "IXC", "IYH", "IDU", "IJU", "IAU", "QUAL", "EFG", "^GSPC"]

    before = int(time.mktime(date(1990,1,1).timetuple()))# Some starting date so I dont download all
    today=int(time.time())

    for ticker in etfs:
        url=f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={before}&period2={today}&interval=1d&events=history&includeAdjustedClose=true"
    
        temp_df=pd.read_csv(url)
        temp_df.loc[1:, (f'{ticker}_ret')]=temp_df['Adj Close'][1:].values/temp_df['Adj Close'][:-1].values-1.0
        temp_df=temp_df.rename(columns={"Date": "date", "Open": "open", "High" : "high",
                                            "Low" : "low", "Close" : "close", "Adj Close" : "adjclose",
                                            "Volume" : "volume"})
        temp_df=temp_df.rename(columns={'date' : 't_day'})
        temp_df=temp_df.filter(['t_day', f'{ticker}_ret'])
        
        temp_df.to_csv(os.path.join(PATH_TO_SEC_DATA, "yahoo_finance", "data_etfs", f"{ticker}.csv"), index=False)
        df=df.merge(temp_df, how='left',on=['t_day'])
    
    # use asreg from stata for the beta
    df=df.sort_values(['cik', 't_day'])

    covariances=df.groupby(['cik'])[['ret', '^GSPC_ret']].rolling(252, min_periods=0).cov().unstack(2)
    
    icov=covariances['^GSPC_ret'].reset_index()
    df['beta']=icov['ret']/icov['^GSPC_ret']

    #Carry forward the beta
    df=df.sort_values(['cik', 't_day'])
    df['beta'] = df.groupby(['cik'])['beta'].ffill()

    df['me']=df['adjclose']*df['cshoq']

    df['avg_size']=df.groupby(['cik'])[['me']].rolling(30, min_periods=0).mean().reset_index()['me'] #.unstack(2)
    #computes 30 days average
    df['bm']=df['seqq']/df['avg_size']
    df['me']=np.log(df['me'])

    df['avg_ret']=df.groupby(['cik'])[['ret']].rolling(30, min_periods=0).mean().reset_index()['ret'] #.unstack(2)
   
    df['prof']=df['oiadpq']/df['atq']
  
    df['cash']=df['cheq']/df['atq']

    df['avg_beta']=df.groupby(['cik'])[['beta']].rolling(30, min_periods=0).mean().reset_index()['beta'] #.unstack(2)
   

    # mret?
    df['prc-7']=df.groupby(['cik']).adjclose.shift(7)
    df['prc-21']=df.groupby(['cik']).adjclose.shift(21)
    df['prc-180']=df.groupby(['cik']).adjclose.shift(180)

    df['mret7']=df['adjclose']/df['prc-7']-1.0
    df['mret21']=df['adjclose']/df['prc-21']-1.0
    df['mret180']=df['adjclose']/df['prc-180']-1.0
    #! No, forward variables only created at running time df['Fret']=df['prc+21']/df['adjclose']-1.0
    # # Stability of the data
    # temp=df[df.cik==320193]

    # plt.plot(temp.t_day, temp.ret)

    tic_unique=set(df.ticker)
    #%%

    # Compute market beta

    # Some analysis of the database?
    print(f"Memory Usage: {df.memory_usage()}")
    print(f"Number of rows: {len(df)}")
    print(f"Sample: {min(df.t_day)} - {max(df.t_day)}")
    print(f"Companies: {len(tic_unique)}")
    #%%
    # Persistence, csv seems still like the best alternative

    # build variables here

    # Save them year after year, we might use maximum data from 2 years for normal signal computations
    df['year']=df.t_day.apply(lambda x: int(x[:4]))
    for y in tqdm(range(min(df.year), max(df.year)+1)):
        temp=df[df.year==y]
        temp.to_csv(os.path.join(PATH_TO_SEC_DATA, f'information_set{y}.csv' ), index=False)
# %%
def aggregate_yf_csv():

    df=pd.DataFrame()
    for i in tqdm(range(os.cpu_count())):
        temp=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, 'yahoo_finance', f"aggregated_yf{i}.csv"))
        # Keep only the relevant variables
        keep_var=['t_day','open','high','low','close','adjclose','volume','ticker',	'ret']
        temp=temp.filter(keep_var)
        df=df.append(temp)

    df.to_csv(os.path.join(PATH_TO_SEC_DATA, 'yahoo_finance', f"aggregated_yf.csv"), index=False)


if __name__=='__main__':
    start=time.time()
    merge_yf_sec()
    end=time.time()
    time_ellapsed=end-start
    t=round(time_ellapsed, 2)
    print(f"{t} seconds")
    




# if __name__=='__main__':

#     start=time.time()
#     df=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "cik_ticker.csv"))
#     tickers=df.ticker
#     nb=os.cpu_count() 
#     batches=np.array_split(tickers,nb)
#     print(f"Batches of size {[len(batch) for batch in batches]}")

#     print(f"Spliting the sample into {len(batches)} batches")
#     processes = [Process(target=append_yahoo_finance, args=(batches[i], i)) for i in [2]] #range(nb)]

#     for p in processes:
#         p.start()

#     for p in processes:
#         p.join()

#     end=time.time()
#     time_ellapsed=end-start
#     t=round(time_ellapsed, 2)
#     n=len(tickers)
#     print(f"{t} seconds - for {n} tickers")