

# This file stores the AlphaGen Strategies used in Python's Backtrader

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
from AlphaGenStrategies import *
from matplotlib import pyplot as plt
import pickle
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']

from utils import retrieve_portfolio

#%%
# Some methods are depreciated, we want to avoid excesive warnings
import sys

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

#%%


#%%

#%%
class St(bt.Strategy):

    # Default, and keyword parameters of the strategy
    params = dict(
         some_param = True
    )

    def __init__(self):
        print("Loading Data...")
        self.information_sets, self.signal_files = load_data()
        self.last_tickets = []
        self.trades_in_month = 0
        self.current_month = 0




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
        
        if t.weekday() == 3 and self.trades_in_month == 0:

            
            tickers_long, tickers_short, tickers= retrieve_portfolio(t, self.information_sets, self.signal_files)
            
            # Some stocks will go to 0, some will have weights
            # tickers in last that are not in new
            new = tickers_long.tolist() + tickers_short.tolist()
            go_out = [ti for ti in self.last_tickets if ti not in new]

            # load only the data
            d_with_len = self.d_with_len
            d_go_out = [d for d in d_with_len if d._name in go_out]

            d_new    = [d for d in d_with_len if d._name in new]

            self.last_tickets = new
            for i,d in enumerate(d_go_out):
                dn = d._name
                    
                # Weights
                price = d.close[0]
                pos = self.getposition(d).size
                if  not np.isnan(price) and not pos:
                        
                    self.order = self.order_target_percent(data=d, target = 0.0 )
                    pos = self.getposition(d).size 
                    weight_in_port = 100*pos*price/self.broker.get_value()
                                
                    value_port = self.broker.get_value()
                
                    print("{} {:15s} Price {:8.2f} Shares {:6.1f} Weight (%) {:6.1f} Value Portfolio {:6.1f} Close Position".format(t, dn, 
                                                                                                                             price, 
                                                                                                                             pos,
                                                                                                                             weight_in_port,
                                                                                                                             value_port))
            for i,d in enumerate(d_new):
                dn = d._name
                    
                target = 0.0
                if dn in tickers_long.tolist():
                    target = 1/(len(tickers_long) + len(tickers_short))
                if dn in tickers_short.tolist():
                    target = -1/(len(tickers_long) + len(tickers_short))
                # Weights
                price = d.close[0]

                prices = d.close

                if  not np.isnan(price):
                        
                    self.order = self.order_target_percent(data=d, target = target )
                    pos = self.getposition(d).size 
                    weight_in_port = 100*pos*price/self.broker.get_value()
                                
                    value_port = self.broker.get_value()
                
                    print("{} {:15s} Price {:8.2f} Shares {:6.1f} Weight (%) {:6.1f} Value Portfolio {:6.1f}".format(t, dn, 
                                                                                                                    price, 
                                                                                                                    pos,
                                                                                                                    weight_in_port,
                                                                                                                    value_port))
                    # Write here, line finishes
                    self.trades_in_month += 1 
           