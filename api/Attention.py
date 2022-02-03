#%%
import numpy as np
import pandas as pd
import os
from tqdm import tqdm
import pickle
PATH_TO_SEC_DATA=os.environ['PATH_TO_SEC_DATA']

# Unpickle attention

with open(f"{PATH_TO_SEC_DATA}\\sentiment\\ASVI_russell1000_v2.pickle", "rb") as f:
    data = pickle.load(f)
# %%
