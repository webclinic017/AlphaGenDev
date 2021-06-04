#--------------------------------------------------------
# PROGRAM NAME - CryptoData.py 
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.0.1    [Mayor].[Minor].[Patch]
#
# PROJECT - AlphaGen 
#
# USAGE - Obtains Crypto data
#
# REQUIRES - Python 3.9<=
#
# SYSTEM - All
#
# DATE - May 30 2021 (Tue) 
#
# BUGS - Not known
#	
#
# DESCRIPTION - Obtains crypto data
#			
#
# Log -  
#--------------------p=E[mx]------------------------------

#%%
import pandas as pd 
import numpy as np
import datetime
import requests
import urllib
from tqdm import tqdm
from tqdm import trange
from bs4 import BeautifulSoup
import os
import time
from multiprocessing import Process, Queue
from selenium import webdriver

PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']

#%%
info={}
# What are all the currencies available?
for k in range(1,12):
    url=f'https://coinmarketcap.com/coins/?page={k}'
    r=requests.get(url)
    soup=BeautifulSoup(r.text, 'html.parser')
    soup.find_all('li', {'class':'page active'})

    rows=soup.find('table').find('tbody').find_all('tr')

    for row in rows:
        url_=row.find_all('a')[0]['href']
        info[url_[12:-1]]=url_
    

#%%
base_url="https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical"
before = int(time.mktime(datetime.date(2010,1,1).timetuple()))
today=int(time.time())

ids=range(1,len(info)+1)
failed=0
for id in tqdm(ids):

    time.sleep(1)
    url=f"{base_url}?id={id}&convertId=2781&timeStart={before}&timeEnd={today}"
    r=requests.get(url)
    result=r.json()
    try:
        df=pd.DataFrame()
        symbol=result['data']['symbol']
        name=result['data']['name']
        for row in result['data']['quotes']:
            temp=row['quote']
            temp={k : [temp[k]] for k in temp.keys()}
            temp['name']=[name]
            temp['symbol']=[symbol]
            df=df.append(pd.DataFrame.from_dict(temp))

        df.to_csv(os.path.join(PATH_TO_SEC_DATA, f"crypto/{symbol}.csv"))
    except:
        failed=failed+1
    
print(f"{failed} - not found")
    

   


# %%
