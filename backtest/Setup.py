#%%
import pandas as pd
import numpy as np
import alpaca_trade_api as tradeapi
import os
import threading
# %%

# authentication and connection details
# All details are stored through environmental variables
# for security

# instantiate REST API
api = tradeapi.REST(api_version='v2')

# obtain account information
account = api.get_account()
print(account)

# %%
aapl = api.get_barset('AAPL', 'day')
tsla = api.get_barset('TSLA', '15Min')
# %%
conn = tradeapi.stream2.StreamConn()

@conn.on(r'^account_updates$')
async def on_account_updates(conn, channel, account):
    print('account', account)

@conn.on(r'^trade_updates$')
async def on_trade_updates(conn, channel, trade):
    print('trade', trade)

def ws_start():
	conn.run(['account_updates', 'trade_updates'])

#start WebSocket in a thread
ws_thread = threading.Thread(target=ws_start, daemon=True)
ws_thread.start()
# %%
