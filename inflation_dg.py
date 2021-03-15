# imports macro data from BBG


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd
import requests
from functions import FREDfuncs as ff

#import dgfuncs as funcs


#datapath1 = r'C:/data/BBG_data/'
#datapath2 = r'C:/data/nonBBG_data/'

inf_dict={
    'UScpi': 'CWUR0000SA0',
    'JPcpi': 'JPNCPALTT01IXNBM'
    }

#BBGinf_df = pd.read_excel(
#    f'{datapath1}inflation.xlsx',
#    index_col=0,
#    parse_dates=True
#    )

#BBGinf_df.columns = [inflation_label_dict[col] for col in BBGinf_df.columns]

#create US inflation series
#BBGinf_df['USinf']=BBGinf_df['UScpi'].pct_change(12)

# resample to business daily frequency
#BBGinf_df = BBGinf_df.resample('B').last().fillna(method='ffill',limit = 40)

### LOAD FRED DATA ###

for key, value in inf_dict.items():
    inf_dict[key] = ff.get_fred(value, key)

cpi_df = pd.concat(inf_dict, axis=1, names=['price_level', 'duplicate']).droplevel('duplicate', axis=1).astype(float)

minf_df = cpi_df.pct_change(fill_method=None)

ainf_df = cpi_df.pct_change(periods=12, fill_method=None)

df = pd.concat([cpi_df, minf_df, ainf_df], axis=1, keys=['levels', 'mom', 'yoy'])
df = df.resample('B').last().ffill(limit=23)

df.to_pickle('C:/Data/pickles/inflation_pickles.pkl')


