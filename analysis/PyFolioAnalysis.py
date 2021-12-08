#%%
import pandas as pd
import numpy as np
import pyfolio as pf
from datetime import datetime
# %%
df = pd.read_csv("../LS25-25SP-p1-S1-l95-xb.csv")
df.date = df.date.apply(lambda x : datetime.strptime(x, "%Y-%m-%d"))

# Fill the gaps with the equivalent return
r = pd.date_range(start=df.date.min(), end=df.date.max())
df = df.set_index('date').reindex(r).fillna(0.0).rename_axis('date').reset_index()
df.head()
# %%
pf.create_full_tear_sheet(df.ret_portfolio)
# %%
df['week'] = df.date.apply(lambda x : f"{x.year} {x.isocalendar()[1]}")

df = df.join(df.groupby('week')['ret_portfolio'].mean(), on='week', rsuffix='_avg')
# %%
df = df.rename(columns = {'ret_portfolio_avg' : 'ret'} )
# %%
df = df.set_index('date')
# %%
