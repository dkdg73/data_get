# imports macro data from BBG

import funcs

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd

datapath1 = r'C:/data/BBG_data/'
datapath2 = r'C:/data/nonBBG_data/'


inflation_label_dict={
    'CPURNSA Index':'UScpi'
    }

BBGinf_df = pd.read_excel(
    f'{datapath1}inflation.xlsx',
    index_col=0,
    parse_dates=True
    )

BBGinf_df.columns = [inflation_label_dict[col] for col in BBGinf_df.columns]

#create US 
BBGinf_df['USinf']=BBGinf_df['UScpi'].pct_change(12)

# resample to daily frequency
BBGinf_df = BBGinf_df.resample('d').asfreq().fillna(method='ffill',limit = 40)

# resample to business daily frequency
BBGinf_df = BBGinf_df.resample('d').asfreq().fillna(method='ffill',limit = 40)

BBGinf_df.to_pickle('C:/Code/asset_allocation/inflation_pickles.pkl')