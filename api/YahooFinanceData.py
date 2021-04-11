#--------------------------------------------------------
# PROGRAM NAME - YahooFinanceData.py 
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.0.1    [Mayor].[Minor].[Patch]
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
# Log - 
# 	
#--------------------p=E[mx]------------------------------

import pandas as pd 
import numpy as np
import time, datetime, requests, urllib
from tqdm.auto import tqdm
from tqdm import trange
from bs4 import BeautifulSoup
import os

PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']
PATH_TO_COMPUSTAT_CRSP_DATA=os.environ['PATH_TO_COMPUSTAT_CRSP_DATA']

def get_yahoo_finance_data():
    df=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "cik_ticker.csv"))

    # Not going before 1990 to avoid overloading
    before = int(time.mktime(datetime.date(1990,1,1).timetuple()))# Some starting date so I dont download all
    today=int(time.time())

    yahoo_finance=pd.DataFrame()
    t = tqdm(df.ticker)
    not_found=0
    for ticker in t:
        url=f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={before}&period2={today}&interval=1d&events=history&includeAdjustedClose=true"
        try:
            temp_df=pd.read_csv(url)
            time.sleep(1)
        except urllib.error.HTTPError: 
            not_found+=1
            t.set_description(f"{ticker.upper()} not found - {not_found} in total")
            t.refresh() # to show immediately the update
            continue

        # we need to include 
        # tic	t_day	t_month	ret
        # A	04mar2021	2021m3	

        temp_df['tic']=ticker.upper()
        temp_df['ret']=[np.nan for i in temp_df['Adj Close']]
        temp_df.loc[1:, ('ret')]=temp_df['Adj Close'][1:].values/temp_df['Adj Close'][:-1].values-1.0


        temp_df['year']=[datetime.datetime.strptime(date, '%Y-%m-%d').year for date in temp_df.Date]
        temp_df['month']=[datetime.datetime.strptime(date, '%Y-%m-%d').month for date in temp_df.Date]
        temp_df['day']=[datetime.datetime.strptime(date, '%Y-%m-%d').day for date in temp_df.Date]

        # I want to make sure column names are 
        # date	open	high	low	close	adjclose	volume	tic	t_day	t_month	ret
        temp_df=temp_df.rename(columns={"Date": "date", "Open": "open", "High" : "high",
                                "Low" : "low", "Close" : "close", "Adj Close" : "adjclose",
                                "Volume" : "volume"})


        yahoo_finance=yahoo_finance.append(temp_df)
        
    yahoo_finance.to_csv(os.path.join(PATH_TO_SEC_DATA, "yahoo_finance.csv"), index=False)


if __name__=='__main__':
    get_yahoo_finance_data()