#%%
# This file provides all the extra functionality of the strategies being tested

import backtrader as bt
import os
import calendar
from datetime import datetime
from datetime import timedelta
from datetime import date
import yfinance as yf
import pandas as pd
import numpy as np
from tqdm import tqdm
import pyfolio as pf
from matplotlib import pyplot as plt
import pickle
import time
from scipy import stats
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']

import sys

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")
#%%
# Fast decorator to time functions
def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()        
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000))
        return result    
    return timed
#%%

def load_data(re_load = False, minyy=2009):
    # Utils, we create the information set in parquet format as csv takes too much to load
    signal_files=os.listdir(f"{PATH_TO_SEC_DATA}\\signals\\v1")
    #Dates.Date(
    signal_files=[datetime.strptime(file[6:-4], "%Y-%m-%d") for file in signal_files if file[0:6]=="signal"]

    years = [d.year for d in signal_files]
    miny = min(years)
    maxy = max(years)

    if re_load:
        for y in tqdm(range(miny,maxy+1)):
            df = pd.read_csv(f"{PATH_TO_SEC_DATA}\\information_set{y}.csv", low_memory=False) 
            df.to_parquet(f"{PATH_TO_SEC_DATA}\\information_set{y}.gzip", compression = 'gzip')

    #%%def clean_companies(df):
    # For what days we have signals?
    # Read the files from parquet
    information_sets=[pd.read_parquet(f'{PATH_TO_SEC_DATA}\\information_set{y}.gzip', engine='pyarrow') for y in range(minyy,maxy+1)] 
    return information_sets

def retrieve_portfolio(t, information_sets, 
                       DD_tile = 2, 
                       pliquid = 90, 
                       minprice = 2, 
                       nl = 25, 
                       ns = 25,
                       type_signal = 'Eret',
                       miny = 2010,
                       correct_precision = False):
    """Function that retrieves a portfolio to invest at time t, to avoid excessive
       data preallocation by default we start in 2010

    Args:
        t (datetime): Year month day date
        information_sets (list of DataFrame): Preallocation of data
        signal_files (list of string): Where are these data?
        DD_tile (int, optional): [Percentile on the DrawDowns to filter]. Defaults to 5.
        pliquid (int, optional): [Percentil on illiquidity above which to discard]. Defaults to 90.
        minprice (int, optional): [Minimum price to consider]. Defaults to 2.
        nl (int, optional): [Number of long stocks]. Defaults to 25.
        ns (int, optional): [Number of short stocks]. Defaults to 25.
        type_signal (str, optional): [What to use as signal]. Defaults to 'Eret'.
        miny (int, optional): Where to start retrieving the tickers. 

    Returns:
        tickers_long: nl tickers to go long
        tickers_short: ns tickers to go short
        df.ticker : all tickers after filtering
    """
   
    y = t.year    
  
    information_set=[i_set for i_set in information_sets if i_set.iloc[0].year == y][0]
    
    try:
        information_set.t_day = information_set.t_day.apply(lambda x : datetime.strptime(x, '%Y-%m-%d'))
    except:
        pass
    #print(information_set.t_day)
    df = information_set[information_set.t_day < pd.to_datetime(t)]
    #print(df.t_day)
    df = df[['t_day', 'open', 'adjclose', 'volume', 'ticker', 'ret', 'cshoq', 'sic', 'beta']]

    # append
    if y > miny:
        df_a = [i_set for i_set in information_sets if i_set.iloc[0].year == y - 1][0] 
        df_a = df_a[['t_day', 'open', 'adjclose', 'volume', 'ticker', 'ret', 'cshoq', 'sic', 'beta']]
        try:
            df_a.t_day = df_a.t_day.apply(lambda x : datetime.strptime(x, '%Y-%m-%d'))
        except:
            pass
        #print(df_a.t_day)
        df= pd.concat([df, df_a]) 

    maxt = max(df.t_day)
    #print(f"Comparing {type(t)} {t} vs {type(maxt)} {maxt}")
    assert t>maxt # Make sure we add the one before and not after
    #We keep the last month of observations to compue a measure of liquidity
    df = df.dropna(axis=0, how='any', thresh=None, subset=['volume', 'adjclose', 'ret', 'cshoq'], inplace=False)
    df2 = df[(df.t_day >= maxt - timedelta(30*3)) & (df.t_day <= maxt) ]
    df2 = df2[['ticker', 't_day', 'open', 'adjclose', 'volume', 'ret', 'cshoq', 'sic', 'beta']]

    df2=df2.sort_values(by=['ticker', 't_day'])
    df2_agg                     = pd.DataFrame()

    df2_agg['adjclose_minimum'] = df2.groupby(by = 'ticker').adjclose.min()
    df2_agg['adjclose_maximum'] = df2.groupby(by = 'ticker').adjclose.max()
    df2_agg['adjclose_last']    = df2.groupby(by = 'ticker').adjclose.last()
    df2_agg['DD']               = df2_agg.adjclose_last/df2_agg.adjclose_maximum - 1.0
    df2_agg.reset_index(inplace=True) 
    #print(df2_agg.head())  
    df2_agg                     = df2_agg[['ticker', 'DD']]

    df = df[(df.t_day >= maxt-timedelta(15)) & (df.t_day<=maxt)]
    df = df[['ticker', 't_day', 'open', 'adjclose', 'volume', 'ret', 'cshoq', 'sic', 'beta']]

    df                     = df.sort_values(by=['ticker', 't_day'])  
    df['me']               = df.adjclose * df.cshoq  
    df['illiq']            = np.abs(df.ret * df.me)/(df.adjclose * df.volume)
    df                     = df.dropna(axis=0, how='any', thresh=None, subset=['illiq'], inplace=False)
    # debugging df = df[df.ticker == 'a']
    df_agg                     = pd.DataFrame()

    df_agg['illiq_mean']       = df.groupby('ticker').illiq.mean()
    df_agg['volume_minimum']   = df.groupby('ticker').volume.min()
    df_agg['me_last']          = df.groupby('ticker').me.last()
    df_agg['beta_last']        = df.groupby('ticker').beta.last()
    df_agg['sic_last']         = df.groupby('ticker').sic.last()
    df_agg['adjclose_last']    = df.groupby('ticker').adjclose.last()
    df_agg['volume_last']      = df.groupby('ticker').volume.last()
    df_agg['adjclose_minimum'] = df.groupby('ticker').adjclose.min()   
    df_agg.reset_index(level=0, inplace=True)


    yyyy = t.year
    mm   = t.month
    dd   = t.day
        
    signal = pd.read_stata(f"{PATH_TO_SEC_DATA}\\signals\\v1\\signal{yyyy}-{mm}-{dd}.dta")
    
    signal = signal[['ticker', 'Fret', 'Eret', 'fe', 'sd_resid', 'Fret_extended', 'Eret_extended', 'fe_extended', 'sd_resid_extended']]
    df     = pd.merge(signal, df_agg, on =['ticker'])
    df     = pd.merge(df,    df2_agg, on =['ticker'])
    df     = df.sort_values(by = ['ticker'])

    # Before Optimization
    df = df.dropna()

    #%%
    df = df[df.DD >= np.percentile(df.DD, DD_tile, axis=0)]
    #%%
    df=df[df.illiq_mean <= np.percentile(df.illiq_mean, pliquid)]
    #%%
    df=df[df.adjclose_last >= minprice ] 
    df=df[df.volume_last > 0 ]

    #%%
    # Get stocks sorted
    # sort on Signal and take top ns and bottom nl
    df['signal'] = df[type_signal]

    if correct_precision:
        # is it an extended signal?
        # Drop the extremes
        
        df['signal'] =  df[type_signal] if 'extended' in type_signal else  df[type_signal] 

        #df['signal']  = df['signal'][df['signal'].between( df['signal'].quantile(.001),  df['signal'].quantile(.999))] 
        df = df[(np.abs(stats.zscore(df['signal'])) < 2.5)]
    
    df = df.dropna(subset=['signal'])
    df = df.sort_values(by = ['signal'])

    tickers_long  = df.ticker[-(nl+1):-1] 
    tickers_short = df.ticker[:ns]
    #%%
    return tickers_long, tickers_short, df.ticker



def filter_tickers():
    """
        return the tickers that actually we might use expost, 
        this function is designed to be ran only from time to time
    """
    information_sets, signal_files = load_data()

    start_date = datetime(2010, 1, 1)
    end_date   = datetime.today()
    all_tickers=np.array([])
    print("Retrieving valid tickers")
    trades_in_month = 0
    for n in tqdm(range(int((end_date - start_date).days))):
        t = start_date + timedelta(n)

        # Is it a new month?
        new_month = t.month != (t-timedelta(1)).month
        if new_month:
            trades_in_month = 0

        if (t.weekday() == 3) and (trades_in_month == 0):
            
            tickers_long, tickers_short, tickers= retrieve_portfolio(t, information_sets, signal_files)
            all_tickers = np.concatenate([all_tickers, tickers])
            trades_in_month += 1
           

    return np.unique(all_tickers)

if __name__ == '__main__':
    all_tickers = filter_tickers()

    # Pickle the relevant tickers
    filename = f'{PATH_TO_SEC_DATA}/cerebros/tickers'
    outfile = open(filename,'wb')
    pickle.dump(all_tickers,outfile)
    outfile.close()
    print(f"Tickers pickled in {PATH_TO_SEC_DATA}/cerebros/tickers")
