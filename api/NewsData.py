# -*- coding: utf-8 -*-
"""
Created on Sat Jul 24 14:11:28 2021

@author: Alperen Canbey
"""

#%% Set up selenium and chrome

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import os
import random
import bs4 as BeautifulSoup
import re
import docx

# @juan
import pandas as pd

#%% Chrome settings
chrome_options = Options()
chrome_options.headless = False
chrome_options.add_argument('--incognito')
chrome_options.add_argument("--window-size=1920,1200")

## Set the browser
#r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe'
path = r'C:\Users\u109898\Documents\chromedriver_win32\chromedriver.exe'
browser = webdriver.Chrome(options = chrome_options, executable_path = path)

#%% Query settings
# @juan
# Suggestion: Use [] to initialize an empty array (works a bit faster)
titlelist  = [] 
textlist   = []
sourcelist = []
datelist   = []
queries    = []

# @juan
# Since the code is running in incognito google requires
# accepting terms and conditions
browser.get("http://www.google.com")
browser.find_elements_by_class_name('jyfHyd')[1].click()


#%%
query = 'APPLE Inc'
for y in range(2010,2011):
    for m in range(1,3):
        time.sleep(1)
        titles = "start"
        page = 0

        print(f"Scraping news for {y}-{m}")
        while len(titles) > 0:
            
            link = 'https://www.google.com/search?q={}&hl=en&tbs=cdr:1,cd_min:{}/1/{},cd_max:{}/1/{}&tbm=nws&start={}'.format(query,m,y,m+1,y,page)
            browser.get(link)
    
        
            #%% Crawler process 
            
            soup = BeautifulSoup.BeautifulSoup(browser.page_source, 'html.parser')
            content = soup.prettify()
            
            """ it is better to look at content and the structure of html file when doing web crawler
            """
             
            ## Find titles
            titles = re.findall('<div aria-level="2" class="JheGif nDgy9d" role="heading" style="-webkit-line-clamp:2">\n\ +(.+)', content)
            
            texts = re.findall('<div class="Y3v8qd">\n\ +(.+)', content)
            
            source = re.findall('</g-img>\n\ +(.+)', content)
            sources = [source[i] for i in range(len(source)) if source[i].find('</div>')==-1 and source[i].find('<g-img>')==-1 and source[i].find('Languages')==-1]
            
            date = re.findall('<span>\n\ +(.+)', content)
            dates = [date[i] for i in range(len(date)) if date[i].find('Languages')==-1 and date[i].find('<em>')==-1 and date[i].find(':')==-1]
            
            qs = [query for s in sources]
            titlelist  += titles
            textlist   += texts
            sourcelist += sources
            datelist   += dates
            queries    += qs
           
           # @juan
            if page > 2:
                break

             page = page + 1
            
            time.sleep(2)

#%%
dict = {'title': titlelist, 'text': textlist, 'source': sourcelist, 'date': datelist, 'company': queries}
df=pd.DataFrame(dict)

df.to_csv("news.csv", index=False)

## find all links
##urls = re.findall('href="(.+)" onmousedown', content)

##all_urls = re.findall('href="(.+)"', content)

# Drop Links to Google services and advertising links, such as youtube, mediamarkt, fnac...
##urls_valid = [urls[i] for i in range(len(urls)) if urls[i].find('google')==-1 and urls[i].find('youtube')==-1 and urls[i].find('mediamarkt')==-1 and urls[i].find('fnac')==-1]

## Save all pages of search
##pages = [all_urls[i] for i in range(len(all_urls)) if all_urls[i].find('search?q')==1]

# """
# To be done:
#     1. Turn to next page: maybe looking for something related to "next"...
#     eğer contentte next varsa diiğer sayfaya geç
#     veya 0 title varsa bitir
#     2. Loop over all pages, and generate a random break among loops
#     3. Loop over all products of interest
    
#     4.sadece 10 tane mi alıyo bütün sayfa mı 
#     5.  +(.+) ne işe yarar
#     6. translate issues
#     7. titles text source ve text aynı olmadığında warning ver
#     8. 2010 ararken 2018 haberi geliyo ne yapmalı 
#     9. uniqueleri al veya gün + 7 yap
#     10. niye random kesiliyor? iki tarihin data toplamı eşleşmiyor
# """






# %%
