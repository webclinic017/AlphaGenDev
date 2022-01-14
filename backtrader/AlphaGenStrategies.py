

# This file stores the AlphaGen Strategies used in Python's Backtrader

#%%
# Some methods are depreciated, we want to avoid excesive warnings
import sys

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")


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
import traceback
from matplotlib import pyplot as plt
import pickle
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']

from utils import retrieve_portfolio, load_data

#%%
from datapackage import Package

df_nasdaq = pd.read_csv("https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed.csv")
# print list of all resources:
NASDAQ = [e.lower() for e in df_nasdaq.Symbol]

df_sp500 = pd.read_csv("https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv")
SP500 = [e.lower() for e in df_sp500.Symbol]

#%%
class St(bt.Strategy):

    # Default, and keyword parameters of the strategy
    params = (
        ('type_signal', 'Eret'),
        ('nl', 50),
        ('ns', 50),
        ('correct_precision' , False),
        ('verbose' , True), 
        ('DD_tile' , 5),
        ('pliquid' , 95), 
        ('minprice' , 1.0) 
    )

    def __init__(self):
        print("Initializing the Investment Strategy...")
        print(f"Signal used: {self.params.type_signal}")
        print(f"Number of long  positions: {self.params.nl}")
        print(f"Number of short positions: {self.params.ns}")
        print(f"Correcting the precision dividing by sd {self.params.correct_precision}")
        self.information_sets = load_data()
        self.last_tickets    = []
        self.trades_in_month = 0
        self.current_month   = 0
        self.weight_nasdaq   = []
        self.weight_sp500    = []
        self.weight_rusell   = []
        self.t_dates         = []

        self.dic_positions = {} # Dictionary to keep track of the pnl of a stock

    def __str__(self):
        return f"{self.params.nl}-{self.params.ns}-{self.params.type_signal}-{self.params.correct_precision}"

    def next(self):

        
        # Obtain the signal data for today
        t = self.datetime.date()

        current_month = t.month
        new_month = current_month != self.current_month
        if new_month:
            self.trades_in_month = 0

        self.current_month = t.month

        # Always Assert we have not NaN data which can be caused by some missing values
        assert not np.isnan(self.broker.get_value())
        dic_pos = {}
        if t.weekday() == 3 and self.trades_in_month == 0:
            try:
                if self.params.verbose:
                    print(f'Attempting Trades on {t.strftime("%Y-%m-%d")} Portfolio Value {self.broker.get_value()}')
                tickers_long, tickers_short, tickers= retrieve_portfolio(t, self.information_sets,
                                                                        nl          = self.params.nl, 
                                                                        ns          = self.params.ns,
                                                                        type_signal = self.params.type_signal,
                                                                        correct_precision = self.params.correct_precision )
               
                # Some stocks will go to 0, some will have weights
                # tickers in last that are not in new
                new = tickers_long.tolist() + tickers_short.tolist()
                go_out = [ti for ti in self.last_tickets if ti not in new]

                # load only the data
                d_with_len = self.datas
                d_go_out = [d for d in d_with_len if d._name in go_out] 

                d_new    = [d for d in d_with_len if d._name in new] # These arrays will be now 2*(nl + ns) so we can fill more

                self.last_tickets = new
                for i,d in enumerate(d_go_out):
                    dn = d._name
                    # Weights
                    price = d.close[0]
                  
                    if  not np.isnan(price) and (price != 0.0) :                        
                        pos = self.getposition(d).size 
                        if self.params.verbose:                    
                            print("""{} Closing {:6.1f} Shares of {:15s} at Price {:8.2f} """.format(t, pos, dn, price))
                        self.order = self.order_target_percent(data=d, target = 0.0 )
                
                w_nasdaq = 0.0
                w_sp500  = 0.0
                w_rusell = 0.0

                n_long  = 0
                n_short = 0

                # How many of the d_new I can actually buy?
                d_new2 = [d for d in d_new if not np.isnan(d.close[0]) and (d.close[0] != 0.0)]
                for i,d in enumerate(d_new2):
                    dn = d._name
                        
                    target = 0.0
                    if dn in tickers_long.tolist():
                        target = 1/len(d_new2)
                    if dn in tickers_short.tolist():
                        target = -1/len(d_new2)
                                    
                    price = d.close[0]
                    if  not np.isnan(price) and (price != 0.0):

                        if dn in NASDAQ:
                            w_nasdaq += target
                        elif dn in SP500:
                            w_sp500 += target
                        else:
                            w_rusell += target

                        if (n_long <= self.params.nl) and (target > 0.0):
                            self.order = self.order_target_percent(data=d, target = target )
                            n_long += 1

                        if (n_short<= self.params.ns) and (target < 0.0):
                            self.order = self.order_target_percent(data=d, target = target )
                            n_short += 1

                        pos = self.getposition(d).size 
                        weight_in_port = 100*pos*price/self.broker.get_value()
                                    
                        value_port = pos*price
                        if self.params.verbose:
                            print("""{} {:15s} Price {:8.2f} Shares {:6.1f} Weight (%) {:6.1f} Value in Portfolio {:6.1f} """.format(t, 
                                                                    dn, 
                                                                    price, 
                                                                    pos,
                                                                    weight_in_port,
                                                                    value_port))
                # Write here, line finishes
                self.trades_in_month += 1 

                temp = self.weight_nasdaq
                temp.append(w_nasdaq)
                self.weight_nasdaq = temp

                temp = self.weight_sp500
                temp.append(w_sp500)
                self.weight_sp500 = temp

                temp = self.weight_rusell
                temp.append(w_rusell)
                self.weight_rusell = temp

                temp = self.t_dates
                temp.append(t)
                self.t_dates = temp

                # For easyness, I overwrite a csv file with the benchmark data
                df = pd.DataFrame()
                df['Date']     = np.array(self.t_dates)
                df['w_nasdaq'] = np.array(self.weight_nasdaq)
                df['w_sp500']  = np.array(self.weight_sp500)
                df['w_rusell'] = np.array(self.weight_rusell)

                cp = 1 if self.params.correct_precision else 0
                name_st = f"{self.params.type_signal}-{self.params.nl}-{self.params.ns}-{cp}"
                df.to_csv(f"{PATH_TO_SEC_DATA}/cerebros/benchmark_{name_st}.csv", index=False)
            except Exception as e:
                # verbose error
                if self.params.verbose:
                    print(traceback.format_exc())
                #pass
                


           