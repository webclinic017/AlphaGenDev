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
import datetime
import requests
import urllib
from tqdm.auto import tqdm
from tqdm import trange
from bs4 import BeautifulSoup
import os
import time
from multiprocessing import Process, Queue



# %%
# We start with the pricing data and then add the accouting data
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']
PATH_TO_COMPUSTAT_CRSP_DATA=os.environ['PATH_TO_COMPUSTAT_CRSP_DATA']

df_yahoo=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, 'yahoo_finance', 'yahoo_finance1.csv'))
df_yahoo=df_yahoo.rename(columns={'date' : 't_day'})
df_yahoo=df_yahoo.rename(columns={'tic' : 'ticker'})
df_yahoo.ticker=df_yahoo.ticker.apply(lambda x : x.lower())
df_sec=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "fundamentals.csv"))

df_link=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "cik_ticker.csv"))
# %%

# Merges and drop _merge==2, then we carry forward the observation

# First we need the cik, for this one has to be an inner join, all of them should appear

df=df_yahoo.merge(df_link, on=['ticker'])
df=df.merge(df_sec, how='left',on=['cik', 't_day'])

#%% Forward filling of the variables

df=df.sort_values(['cik', 't_day'])

to_ffill=['atq', 'cheq', 'cshoq', 'oiadpq', 'seqq']

for var in to_ffill:
    df[var] = df.groupby(['cik'])[var].ffill()

# %%
 # Some checks on that data

 tic_unique=set(df.ticker)
 tic_uniqueyf=set(df_yahoo.ticker)
# %%
