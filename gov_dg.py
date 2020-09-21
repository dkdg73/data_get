# load & wrangle BBG govt bond index data into a clean workable dataframe for pickling
# takes bloomberg data for contemporary data
# takes BAML data for majority of index attributes (eg yld, duration, spreads)
# historical data from various sources for history
# splices series together where possible to give the longest run of data possible

import funcs

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd

datapath1 = r'C:/data/BBG_data/'
datapath2 = r'C:/data/nonBBG_data/'

bond_dts = ['tri', 'yld']

bond_label_dict={
    'tri':{'G1O2 Index':'USbonds1-3y',	'G2O2 Index':'USbonds3-5y', 'G4O2 Index':'USbonds7-10y',
    'G8O2 Index':'USbonds15y+', 'LGAGTRUH Index':'WDbondagg', 'EG01 Index':'EURbonds1-3y',
    'EG02 Index':'EURbonds3-5y','EG04 Index':'EURbonds7-10y','EG08 Index':'EURbonds15y+'},
    'yld':{'USGG2YR Index':'USbondy2', 'GT5 Govt':'USbondy5',	'USGG10YR Index':'USbondy10',
    'USGG30YR Index':'USbondy30','LGAGTRUH Index':'WDbondagg'}
    }

#load BBG credit data into a dictionary of dfs
BBG_bonddata = pd.read_excel(
    f'{datapath1}govbond.xlsx',
    sheet_name=bond_dts,
    index_col=0,
    parse_dates=True
    )

# concatenate the dictionary into a single df
BBGbond_df = pd.concat(
    [BBG_bonddata['tri'],BBG_bonddata['yld']],
    keys=['tri','yld'],
    names=['datatype','gen_index'], axis=1)

# change the BBGbond_df columns from BBGto CCR generic index names
idx = [(item[0], bond_label_dict[item[0]][item[1]]) for item in BBGbond_df.columns]
BBGbond_df.columns = pd.MultiIndex.from_tuples(idx, names=['datatype', 'gen_index'])

BBGbond_df.to_pickle('C:/Code/asset_allocation/bond_pickles.pkl')
