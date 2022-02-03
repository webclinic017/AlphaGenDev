#%%
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup as bs
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import math
driver = webdriver.Firefox()
driver.get("https://ac.sqrpnt.com/")

datas= driver.find_elements("css selector", "input")
username = datas[0]
password = datas[1]
checkbox = datas[2]
username.send_keys("filippo.ippolito@gmail.com")
password.send_keys("8zmExE!34j8b92w")

checkbox.click()
time.sleep(5)
button= driver.find_elements("css selector", "button")[0]
button.click()
time.sleep(5)

button = driver.find_elements("css selector", "button")[1]
button.click()
name_search = driver.find_elements("css selector","input[id='id-Search ticker or name']")[0]
df = pd.read_csv("2022-01-13_.csv")
# %%
tickers = df.tick_neg
for t in tickers:

    name_search.send_keys(t.upper())
    time.sleep(1)
    list_companies = driver.find_elements("css selector", "ul")[0]
    uls = list_companies.find_elements("css selector", "div")

    # Loop and record the maximum amount
    t_ul = math.floor(len(uls)/3)

    for i in range(t_ul):

    ul=uls[i*3+1]
    ul.click()

    # Look for div to know how much money is available to invest
    divs=driver.find_elements("css selector", ".dialog > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(2) > div:nth-child(1) > div:nth-child(2)")
    amount = float(divs[0].text.replace("USD", "").replace(",",""))
    divs = driver.find_elements("css selector",".dialog > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > h1:nth-child(1)")
    tick = divs[0].text.split(" ")[0].lower()
    bb = driver.find_elements("css selector",".back-button > div:nth-child(1) > svg:nth-child(1)")
    bb[0].click()
    bb = driver.find_elements("css selector",".back-button > div:nth-child(1) > svg:nth-child(2) > path:nth-child(1)")
    bb[0].click()

    # %%
    # list of potential tickers?
    driver.close()

