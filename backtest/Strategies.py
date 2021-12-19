#%%

#%%
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

#%%
# Some methods are depreciated, we want to avoid excesive warnings
import sys

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

#%%
def load_data(re_load = False):
    # Utils, we create the information set in parquet format as csv takes too much to load
    signal_files=os.listdir(f"{PATH_TO_SEC_DATA}\\signals\\v1")
    #Dates.Date(
    signal_files=[datetime.strptime(file[6:-4], "%Y-%m-%d") for file in signal_files if (file[0:6]=="signal") and (int(file[6:10])>=2010)]
    
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
    information_sets=[pd.read_parquet(f'{PATH_TO_SEC_DATA}\\information_set{y}.gzip', engine='pyarrow') for y in range(miny,maxy+1)] 
    return information_sets, signal_files

#%%

def retrieve_portfolio(t, information_sets, signal_files, DD_tile = 0, pliquid = 90, minprice = 2, nl = 25, ns = 25):
    #t = datetime(2021, 11, 25)
    y = t.year
    years = [d.year for d in signal_files]
    miny = min(years)
    maxy = max(years)
    information_set=information_sets[y-miny]
    
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
        df_a = information_sets[y-miny-1] 
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

    sf = np.array(signal_files)
    sf[np.where(np.array(sf) == t)[0]]
    date_signal =  sf[0]

    yyyy = date_signal.year
    mm   = date_signal.month
    dd   = date_signal.day
        
    signal = pd.read_stata(f"{PATH_TO_SEC_DATA}\\signals\\v1\\signal{yyyy}-{mm}-{dd}.dta")
    signal = signal[['ticker', 'Fret', 'Eret', 'fe', 'sd_resid']]
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
    # sort on Eret and take top 50 and bottom 50
    df = df.sort_values(by = ['Eret'])
    #df = df.sort_values(by = ['Fret'])

    tickers_long  = df.ticker[-(nl+1):-1]
    tickers_short = df.ticker[:ns]
    #%%
    return tickers_long, tickers_short, df.ticker
#%%

class St(bt.Strategy):

    def __init__(self):
        print("Loading Data...")
        self.information_sets, self.signal_files = load_data()
        self.last_tickets = []
        self.times_traded = 0
        

   
    def next(self):
        t = self.datetime.date()

        # Increment the # of times traded once a week, since Python doesnt really have a once a month
        # option
        if t.weekday() == 1:
            self.times_traded += 1


        if (t.weekday() == 3) and (self.times_traded % 4 == 0):
            
            tickers_long, tickers_short, tickers= retrieve_portfolio(t, self.information_sets, self.signal_files)
            
            # Some stocks will go to 0, some will have weights
            # tickers in last that are not in new
            universe = [d._name for d in self.datas]
            
            new = tickers_long.tolist() + tickers_short.tolist()
           
            go_out = [ti for ti in self.last_tickets if ti not in new]

            # load only the data
            d_with_len = self.datas
            d_go_out = [d for d in d_with_len if d._name.lower() in go_out]

            d_new    = [d for d in d_with_len if d._name.lower() in new]

            self.last_tickets = new
            for i,d in enumerate(d_go_out):
                dn = d._name
                    
                # Weights
                price = d.close[0]
                
                if  (not np.isnan(price)) and (price != 0.0):
                        
                    self.order = self.order_target_percent(data=d, target = 0.0 )
                    pos = self.getposition(d).size 
                    weight_in_port = 100*pos*price/self.broker.get_value()
                                
                    value_pos = pos*price 
                
                    print("{} {:15s} Price {:8.2f} Shares {:6.1f} Weight (%) {:6.1f} Value Portfolio {:6.1f} Close Position".format(t, dn, 
                                                                                                                             price, 
                                                                                                                             pos,
                                                                                                                    weight_in_port,
                                                                                                                             value_pos))
            for i,d in enumerate(d_new):
                dn = d._name
                    
                target = 0.0
                if dn in tickers_long.tolist():
                    target = 1/50
                if dn in tickers_short.tolist():
                    target = -1/50
                # Weights
                price = d.close[0]

                prices = d.close

                if  not np.isnan(price) and (price != 0.0):
                        
                    self.order = self.order_target_percent(data=d, target = target )
                    pos = self.getposition(d).size 
                    weight_in_port = 100*pos*price/self.broker.get_value()
                                
                    value_pos = pos*price
                
                    print("{} {:15s} Price {:8.2f} Shares {:6.1f} Weight (%) {:6.1f} Value Portfolio {:6.1f}".format(t, 
                                                                                                                    dn,
                                                                                                                    price,
                                                                                                                    pos,
                                                                                                                    weight_in_port,
                                                                                                                    value_pos))
           
            value_port = self.broker.get_value() 
            print(f"Value of Portfolio: {value_port}")



def pickle_cerebro(tickers):
    # Cerebro setup
    cerebro = bt.Cerebro()
    

    # When to start
    initial_date = datetime(2010,1,1)
    
    # Adds commissions
    # 0.1% ... divide by 100 to remove the %

    print(f"Adding {len(tickers)} tickers")

    tickers = tqdm(tickers)
    not_added = 0
    for ticker in tickers:
        tickers.set_description(f"Adding {ticker} - {not_added} tickers failed to meet Cerebro")
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
            
            
                df = df[df.date >= initial_date]
                df.date = df.date.apply(lambda x: x + timedelta(seconds=0, minutes=0, hours=16))
                df.sort_values(on=['date'])
               

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
        except:
            not_added = not_added + 1

    # Pickle CEREBRO
    filename = f'cerebro_St'
    outfile = open(filename,'wb')
    pickle.dump(cerebro,outfile)
    outfile.close()

# util
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield 

def filter_tickers():
    """
        return the tickers that actually we might use expost, 
        this function is designed to be ran only from time to time
    """
    information_sets, signal_files = load_data(re_load = True)

    start_date = datetime(2011, 1, 1)
    end_date   = datetime(2021,11,25)
    all_tickers=np.array([])
    print("Retrieving valid tickers")
    for n in tqdm(range(int((end_date - start_date).days))):
        t = start_date + timedelta(n)
        if t.weekday() == 3:
            
            tickers_long, tickers_short, tickers= retrieve_portfolio(t, information_sets, signal_files)
            all_tickers = np.concatenate([all_tickers, tickers])
           

    return np.unique(all_tickers)

#%%
if __name__ == '__main__':

    all_tickers = filter_tickers()
    #pickle_cerebro(all_tickers)

    # Pickle the relevant tickers
    filename = f'tickers'
    outfile = open(filename,'wb')
    pickle.dump(all_tickers,outfile)
    outfile.close()
