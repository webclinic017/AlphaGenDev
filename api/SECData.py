#--------------------------------------------------------
# PROGRAM NAME - .py 
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.0.1    [Mayor].[Minor].[Patch]
#
# PROJECT - AlphaGen 
#
# USAGE - Maintains SEC Information
#
# REQUIRES - Python 3.9<=
#
# SYSTEM - All
#
# DATE - Apr 10 2021  (Sat) 12:05
#
# BUGS - Not known
#	
#
# DESCRIPTION - Imports data from the sec api and stores it in a database
#			
#			
#--------------------p=E[mx]------------------------------
#%%
import requests 
import zipfile
import io
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import pickle
#%%
# Environmental Variables e.g. paths to files, api keys, credentials etc
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']
PATH_TO_COMPUSTAT_CRSP_DATA=os.environ['PATH_TO_COMPUSTAT_CRSP_DATA']
#%%
def download_zip_sec(url, save_path, chunk_size):
    try:
        r = requests.get(url, stream=True)
        with open(save_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)

        with zipfile.ZipFile(save_path, 'r') as zip_ref:
            zip_ref.extractall(save_path.replace(".zip",""))

        os.remove(save_path)
        print(f"Financial Data from {url} downloaded...")
    except:
        pass
#%%
def download_url_sec(y, q, chunk_size=128):
    save_path=f"{PATH_TO_SEC_DATA}/sec{y}{q}.zip"
    url=""
    if y!=2022:
        url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{y}q{q}.zip"
        download_zip_sec(url, save_path, chunk_size)
    else:
        url = f"https://www.sec.gov/files/node/add/data_distribution/{y}q{q}.zip"
        download_zip_sec(url, save_path, chunk_size)

        

#%%   


def update_all_data():
    years=range(2021, datetime.today().year+1)
    quarters=[1,2,3,4]
    for y in years:
        for q in quarters:
            download_url_sec(y,q)



#%%


def prepare_sec(y,q):
    main_path=os.path.join(PATH_TO_SEC_DATA, f"sec{y}{q}")
    # If we dont have the folder we dont try to rpocess the data
    if not os.path.exists(main_path):
        return
    num = pd.read_csv(os.path.join(main_path, "num.txt"), sep="\t")
    sub = pd.read_csv(os.path.join(main_path, "sub.txt"), sep="\t")
    tag = pd.read_csv(os.path.join(main_path, "tag.txt"), sep="\t")

    # Sometimes files directly from SEC are empty, e.g. 2009q1 so we just break
    if len(num)==0:
        return

    # From Dataframe num we keep accounts that have qtrs in [0,1,4] e.g. balance sheet and income statement
    num=num[(num.qtrs == 0) | (num.qtrs == 1) | (num.qtrs == 4)]

    # We focus on very specific accounts
    accounts=["Assets","StockholdersEquity", "CommonStockSharesOutstanding", "OperatingIncomeLoss","CashAndCashEquivalentsAtCarryingValue"]
    num=num[num.tag.isin(accounts)]

    # now we merge (inner join e.g. keep if _merge==3 in Stata) with sub
    df = pd.merge(num,sub,on='adsh')

    # Keep only relevant variables
    relevant_variables=['cik', 'ddate', 'tag', 'value', 'qtrs', 'accepted']
    df=df[relevant_variables]
    df['t_day']=pd.to_datetime(df.accepted, format='%Y-%m-%d')

    # Now, info is only available to the public until next day of being accepted
    df['t_day']=df['t_day']+timedelta(days=1)

    # We keep the one referring to the latest date of report, 
    df=df.sort_values(['cik','tag', 't_day', 'ddate'])
    df=df.groupby(['cik', 'tag', 't_day']).tail(1)

    # Prepare to reshape data
    df= df.rename(columns={'value': 'v'})
    df= df.drop(['qtrs'], axis=1)
        
    df=df.drop_duplicates(subset=['cik','tag','ddate','t_day'], keep='last')

    di={"Assets" : "atq",
        "StockholdersEquity" : "seqq",
        "CommonStockSharesOutstanding" : "cshoq",
        "OperatingIncomeLoss" : "oiadpq",
        "CashAndCashEquivalentsAtCarryingValue" : "cheq"}
    df=df.replace({"tag": di})
            
    # drop ddate accepted
    df=df.drop(['accepted'], axis=1)


    df=df.pivot_table(index=["cik", "t_day", 'ddate'], 
                        columns='tag', 
                        values='v').reset_index()
            
    df.to_csv(os.path.join(PATH_TO_SEC_DATA, f"sec{y}{q}", "aggregated.csv"), index=False)


# %%
def process_all_data():
    years=range(2009, datetime.today().year+1)
    quarters=[1,2,3,4]
    for y in years:
        for q in quarters:
            print(f"{y} - {q}")
            prepare_sec(y,q)
# %%

#process_all_data()


#%%
def get_cik_ticker():
    """
    Equivalent to Stata
    import delimited using "https://www.sec.gov/include/ticker.txt", clear
    rename v1 ticker
    rename v2 cik
    duplicates drop cik, force
    save cik_ticker, replace
    """
    # Match cik and tickers
    # csv in url has no headers
    df=pd.read_csv("https://www.sec.gov/include/ticker.txt", sep="\t", header=None)
    df= df.rename(columns={0: 'ticker', 1: 'cik'})
    df=df.drop_duplicates(subset=['cik','ticker'], keep='last')

    df.to_csv(os.path.join(PATH_TO_SEC_DATA, "cik_ticker.csv"), index=False)

# %%
# Add compustat data to complete the sample so that we can do backtests further in time

# Requires another env variable to compustat and crsp data
# Format of the year/quarter date is not the same as in Stata
def link_compustat_crsp():
    """
    use "CompustatCRSP/gvkey_cik", clear
    gen t_quarter=yq(year(datadate), quarter(datadate))
    format %tq t_quarter
    drop if cik==""
    keep gvkey t_quarter cik
    duplicates drop gvkey t_quarter, force
    save "CompustatCRSP/link", replace
    """
    df=pd.read_stata(os.path.join(PATH_TO_COMPUSTAT_CRSP_DATA,"gvkey_cik.dta"))
    df=df[df.cik!='']
    df= df.rename(columns={'datacqtr': 't_quarter'})
    df=df.filter(['gvkey', 't_quarter', 'cik', 'datadate'])
    df=df.drop_duplicates(subset=['gvkey','t_quarter'], keep='last')
    df.to_csv(os.path.join(PATH_TO_SEC_DATA, "link.csv"), index=False)

# %%
