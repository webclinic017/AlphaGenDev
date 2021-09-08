#%%
import pandas as pd
import os
import re
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']

from datetime import datetime
today = datetime.today()
all_df = pd.DataFrame()
for y in range(2009, today.year + 1):
    for q in [1,2,3,4]:
        path = os.path.join(PATH_TO_SEC_DATA, f'sec{y}{q}', 'sub.txt')
        try:
            df = pd.read_csv(path, sep="\t", low_memory=False)
            all_df = all_df.append(df)
        except:
            pass


df = all_df[['cik', 'name', 'period']]
df = df.sort_values(['cik', 'name', 'period'])

mask = df.groupby(['cik', 'name']).cumcount() == 0
df = df[mask]
df.columns=['cik', 'name', 'start_date']

# Some data cleaning
df['name'] = df['name'].apply(lambda x: x.replace('"', '')) 
# df['name'] = df['name'].apply(lambda x: x.replace(' INC', '')) 
# df['name'] = df['name'].apply(lambda x: x.replace(' CORP', '')) 
# df['name'] = df['name'].apply(lambda x: x.replace(' CO', '')) 
df['name'] = df['name'].apply(lambda x: re.sub(r'[^A-Za-z0-9 ]+', '', x))

df.to_csv(os.path.join(PATH_TO_SEC_DATA, 'for_news.csv'), index = False)
# %%
