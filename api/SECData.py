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

#%%
# Environmental Variables e.g. paths to files, api keys, credentials etc
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']
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
    save_path=f"/data/sec{y}{q}.zip"
    url=""
    if y!=2021:
        url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{y}q{q}.zip"
        download_zip_sec(url, save_path, chunk_size)
    else:
        url = f"https://www.sec.gov/files/node/add/data_distribution/{y}q{q}.zip"
        download_zip_sec(url, save_path, chunk_size)

#%%   
# def process_sec(y,q):
#     pass
y=2021
q=1

num = pd.read_csv(f"../data/sec{y}{q}/num.txt", sep="\t")
sub = pd.read_csv(f"../data/sec{y}{q}/sub.txt", sep="\t")
tag = pd.read_csv(f"../data/sec{y}{q}/tag.txt", sep="\t")

num=num[num.qtrs in [0,1,4],:]
# From Dataframe num we keep accounts that have qtrs in [0,1,4] e.g. balance sheet and income statement
# items

# 		use num, clear
		
# 		keep if qtrs==1 | qtrs==0 | qtrs==4

# 		//tab tag if strpos(tag, "StockholdersEquity")==1carry
		

# 		keep if tag=="Assets" | ///
# 				tag=="StockholdersEquity" | ///
# 				tag=="CommonStockSharesOutstanding" | ///
# 				tag=="OperatingIncomeLoss" | ///
# 				tag=="CashAndCashEquivalentsAtCarryingValue"

# 		merge m:1 adsh using sub
# 		keep if _merge==3
# 		drop _merge
