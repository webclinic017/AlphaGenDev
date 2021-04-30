#--------------------------------------------------------
# PROGRAM NAME - SECData.py 
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
# Log - 
# 
# 11/04/2021	
# It turns out that the time reported in the SEC has an hour component that must
# be taken into account, as daily values might not be unique. Also we need to double ensure that the
# reference to the account is the most recent and not a rectification of a value	
# 
# 
# 22/04/2021
# SIC codes did not appear on the compustat database we used, so im adding them from the SEC files	
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
import pathlib
import matplotlib.pyplot as plt
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
    years=range(2009, datetime.today().year+1)
    quarters=[1,2,3,4]
    for y in years:
        for q in quarters:
            download_url_sec(y,q)


# %%
#* No longer necessary, as prepare_sec(y,q) keeps the value
def get_sic_codes():
    sics=pd.DataFrame()
    for y in range(2009, datetime.today().year+1):
        for q in range(1,5):
            try:
                df=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, f"sec{y}{q}", "sub.txt"), sep="\t")
                df.accepted = df.accepted.apply(lambda x : x[:-11])
                df['t_day']=pd.to_datetime(df.accepted, format='%Y-%m-%d')
                # Now, info is only available to the public until next day of being accepted
                df['t_day']=df['t_day']+timedelta(days=1)
                df=df.filter(['cik', 'sic', 't_day'])
                sics=sics.append(df)
            except FileNotFoundError:
                pass
    return sics

#%%
def prepare_sec(y,q):
    main_path=os.path.join(PATH_TO_SEC_DATA, f"sec{y}{q}")
    # If we dont have the folder we dont try to rpocess the data
    if not os.path.exists(main_path):
        return
    num = pd.read_csv(os.path.join(main_path, "num.txt"), sep="\t", low_memory=False)
    sub = pd.read_csv(os.path.join(main_path, "sub.txt"), sep="\t", low_memory=False)
    tag = pd.read_csv(os.path.join(main_path, "tag.txt"), sep="\t", low_memory=False)

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

    df.accepted = df.accepted.apply(lambda x : x[:-11])

    # Keep only relevant variables
    relevant_variables=['cik', 'ddate', 'tag', 'value', 'qtrs', 'accepted', 'sic']
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


    df=df.pivot_table(index=["cik", "t_day", 'ddate', 'sic'], 
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

def prepare_compustat_data():
    """
    use "CompustatCRSP/Compustat Quarterly Short", clear
    drop if oiadpq==.
    drop if atq==.
    drop if cheq==.
    drop if seqq==.
    gen t_quarter=yq(year(datadate), quarter(datadate))
    format %tq t_quarter
    duplicates drop gvkey t_quarter, force
    merge 1:1 gvkey t_quarter using "CompustatCRSP/link"
    keep if _merge==3
    drop _merge
    // Compute the required variables
    sort gvkey t_quarter
    rename oiadpq voiadpq
    rename seqq vseqq
    rename cheq vcheq
    rename atq vatq
    rename cshoq vcshoq
    rename datadate t_day
    destring cik, force replace
    //For compustat the information is available for investors 6 months later
    sort cik t_day
    replace t_day=t_day+30*6
    keep cik t_day vatq vcheq vcshoq voiadpq vseqq
    save compustat_data, replace
    """
    df=pd.read_stata(os.path.join(PATH_TO_COMPUSTAT_CRSP_DATA,"Compustat Quarterly Short.dta"))

    df=df[  (df.oiadpq !='') &
            (df.atq    !='') &
            (df.cheq   !='') &
            (df.seqq   !='') ]

    df=df.rename(columns={'datacqtr': 't_quarter'})
    df=df.drop_duplicates(subset=['gvkey','t_quarter'], keep='last')

    df_link=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "link.csv"))
    

    df.gvkey = pd.to_numeric(df.gvkey)
    df = pd.merge(df, df_link, on=['gvkey', 't_quarter'])

    df=df.rename(columns={'datadate_x' : 't_day'})

    df=df.filter(['cik', 't_day', 'atq', 'cheq', 'cshoq', 'oiadpq', 'seqq', 'sic'], axis=1)


    # We multiply by 1 million, this seems the difference
    # example compustat 1195933,2015-12-27 ,2465.257, 155.74,11.426,23.175,702.555
    # example sec       1195933,2015-05-08 ,2446702000.0,96434000.0,,,     707328000.0
    var_scale=['atq', 'cheq', 'cshoq', 'oiadpq', 'seqq']
    for var in var_scale:
        df[var]=df[var]*1000000


    # To avoid look ahead bias data moves 6 months, but we keep the infor of when is the report from
    df['ddate']=df.t_day.apply(lambda x: x.strftime("%Y%m%d"))

    df.t_day=df.t_day + timedelta(days=6*30)
    df['source']='compustat'

    sics=get_sic_codes()
    sics=sics.drop_duplicates(['cik'], keep='last').filter(['cik', 'sic'])

    df=pd.merge(df, sics, on=['cik'])
    df.to_csv(os.path.join(PATH_TO_SEC_DATA, "compustat_data.csv"), index=False)


# %%
def aggregate_sec_data():
    years=range(2009, datetime.today().year+1)
    quarters=[1,2,3,4]
    df=pd.DataFrame()
    for y in years:
        for q in quarters:
            print(f"{y} - {q}")
            try:
                to_append=pd.read_csv(os.path.join(PATH_TO_SEC_DATA, 
                                                    f"sec{y}{q}", "aggregated.csv"))
                df=df.append(to_append)
            except:
                pass
    df['source']='sec'
    df.to_csv(os.path.join(PATH_TO_SEC_DATA, "sec_data.csv"), index=False)


#%%
# Append compustat to sec
def append_compustat_sec():
    """
    # # Modifies the profitability by remoing the cumulative component
    # # x is an entire row
    #     by cik: replace voiadpq=voiadpq[_n]-(voiadpq[_n-1]+voiadpq[_n-2]+voiadpq[_n-3]) ///
    #     if quarter(dofq(t_quarter[_n]))==4 ///
    #                    & voiadpq[_n]!=. ///
    #                    & voiadpq[_n-1]!=. ///
    #                    & voiadpq[_n-2]!=. ///
    #                    & voiadpq[_n-3]!=. ///
    #                    & quarter(dofq(t_quarter[_n-1]))==3 ///
    #                    & quarter(dofq(t_quarter[_n-2]))==2 ///
    #                    & quarter(dofq(t_quarter[_n-3]))==1 
    """
    df        = pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "sec_data.csv"))

    to_append = pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "compustat_data.csv"))

    # I only want to append, when data is missing, 
    # * I was trying appending before the minimum but this leads to missing quarters
    # * Better to append and remove duplicates carefully

    df = df.append(to_append)

    df_link = pd.read_csv(os.path.join(PATH_TO_SEC_DATA, "link.csv"))
    df_link=df_link.drop_duplicates(subset=['cik'], keep='last')
    df = pd.merge(df, df_link)

    # An idea is once both sec and compustat data are available to keep only sec
    df['is_sec']=0
    df.loc[df.source == 'sec', 'is_sec']=1

    # Cumulative sum per cik
    # We sort by cik and date
    df=df.sort_values(['cik', 't_day'])

    df['cum_sec'] = df.groupby(['cik']).is_sec.cumsum()

    df=df.drop(['is_sec'], axis=1) # drop is_sec
    # So, if we have had already a sec report, we drop any compustat file
    df=df[~ ( (df.cum_sec >= 1) & (df.source=='compustat'))]
    df=df.drop(['cum_sec'], axis=1) # drop cum_sec
    # We need a variable keeping track of the t_quarter to compute profitability
    df.t_day=pd.to_datetime(df.t_day)
    df['quarter'] = df.t_day.apply(lambda x : x.quarter)
    df['year']    = df.t_day.apply(lambda x : x.year)

    # Create temporally a lag of profitability, since they are cumulative, we compute the difference except the 
    # first one
  

    df=df.sort_values(['cik', 't_day'])
    df['max_ddate']=df.groupby(['cik']).ddate.cummax()
    # Make sure the date reporting is not a revision
    df=df[ ~ (df.ddate < df.max_ddate)]
    df=df.drop(['max_ddate'], axis=1)

    # plt.plot(df.t_day, df.oiadpq)
    # plt.show()

    #
    #df.loc[df.quarter == 4, 'oiadpq'] = df.loc[df.quarter == 4, 'oiadpq'] -(df.loc[df.quarter == 4, 'l1oiadpq'] + df.loc[df.quarter == 4, 'l2oiadpq'] + df.loc[df.quarter == 4, 'l3oiadpq'])
    
    #df.loc[df.quarter != 1, 'oiadpq'] = df.loc[df.quarter != 1, 'l0oiadpq'] -df.loc[df.quarter != 1, 'l1oiadpq']
    # #!* Debugging - track a simple company 
    # df=df[df.cik==858877]
    # df=df.sort_values(['t_day'])
    # # Try now
    # plt.plot(df.t_day, df.oiadpq/df.atq)
    # plt.show()

    # Keep only the releant variables
    var_keep=['cik', 't_day', 'ddate', 'atq', 'cheq', 'cshoq', 'oiadpq', 'seqq',
       'source', 'gvkey', 'sic']
    df=df.filter(var_keep, axis=1)

    df.to_csv(os.path.join(PATH_TO_SEC_DATA, "fundamentals.csv"), index=False)
    

        




# %%
if __name__=='__main__':
    append_compustat_sec()