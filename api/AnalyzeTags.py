# Analyze the systematic change in names between the variables
#%%
import pandas as pd
from collections import Counter
#%%
# Environmental Variables e.g. paths to files, api keys, credentials etc
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']
PATH_TO_COMPUSTAT_CRSP_DATA=os.environ['PATH_TO_COMPUSTAT_CRSP_DATA']


# %%
df = pd.read_csv(os.path.join(PATH_TO_SEC_DATA, 'fundamentals.csv'))
# %%
for c in df.columns:    
    print(c)
# %%
