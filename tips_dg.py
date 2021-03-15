# load & wrangle BBG inflation data into a clean workable dataframe for pickling
# takes bloomberg data for contemporary data
# takes BAML data for majority of index attributes (eg yld, duration, spreads)
# historical data from various sources for history
# splices series together where possible to give the longest run of data possible

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd

#import dgfuncs as funcs

datapath1 = r'C:/data/BBG_data/'
datapath2 = r'C:/data/nonBBG_data/'

tips_dts = ['tri', 'break_even','real_yld']
names=['datatype','gen_index']

tips_label_dict={
    'tri':{'GUQI Index':'UStips0-5y', 'G4QI Index':'UStips7-10y',	'G8QI Index':'UStips15y+',
    'W0GI Index':'WDtips'},
    'break_even':{'USGGBE10 Index':'USbe10','USGGBE30 Index':'USbe30'},
    'real_yld':{'GTII5 Govt':'USrealyld5','GTII10 Govt':'USrealyld10','GTII30 Govt':'USrealyld30'},
    }

#load BBG credit data into a dictionary of dfs
BBGtips_data = pd.read_excel(
    f'{datapath1}tips.xlsx',
    sheet_name=tips_dts,
    index_col=0,
    parse_dates=True
    )

# concatenate the dictionary into a single df
# NB note that the BBGtips_data['tri'] df is cleaned of its zeros
# (BBG data outputs zeros instead of NA's for early data)

BBGtips_df = pd.concat(
    [BBGtips_data['tri'].where(BBGtips_data['tri']>0),BBGtips_data['break_even'],BBGtips_data['real_yld']],
    keys=tips_dts,
    names=names,
    axis=1
    )

# change the BBGtips_df columns from BBGto CCR generic index names
idx = [(item[0], tips_label_dict[item[0]][item[1]]) for item in BBGtips_df.columns]
BBGtips_df.columns = pd.MultiIndex.from_tuples(idx, names=names)

#BBGtips_df.to_pickle('C:/Data/pickles/tips_pickles.pkl')
