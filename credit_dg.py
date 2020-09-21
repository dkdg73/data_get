# load & wrangle BBG credit index data into a clean workable dataframe for pickling
# takes bloomberg data for contemporary data
# takes BAML data for majority of credit index attributes (eg yld, duration, spread)
# historical data from various sources for history
# splices series together where possible to give the longest run of data possible

import dgfuncs

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd

datapath1 = r'C:/data/BBG_data/'
datapath2 = r'C:/data/nonBBG_data/'

credit_dts = ['tri', 'spread', 'yld']

credit_label_dict={
    'tri':{
    'LGCPTRUH':'USDglobalaggcorp', 'BAC1TRUH':'USDglobalaggcorp1-3y', 'LG30TRUH':'USDglobalHY',
    'SRCATTRR':'USDcatbonds','V0S1':'USDconvIG', 'V0S2':'USDconvHY', 'COCU':'USDcocoIG', 'COHY':'USDcocoHY',
    'COCE':'EURcocoHY','C1B0':'USDcorpAAA-AA1-3y', 'C2B0':'USDcorpAAA-AA3-5y', 'C6B0':'USDcorpAAA-AA5-10y',
    'C8B0':'USDcorpAAA-AA15y+', 'C1C0':'USDcorpA-BBB1-3y', 'C2C0':'USDcorpA-BBB3-5y', 'C6C0':'USDcorpA-BBB5-10y',
    'C8C0':'USDcorpA-BBB15y+', 'H100':'USDcorpHY', 'J1A1':'USDcorpBB1-3y','J1A2':'USDcorpB1-3y',
    'J1A3':'USDcorpCCC1-3y', 'J2A1':'USDcorpBB3-5y', 'J2A2':'USDcorpB3-5y', 'J2A3':'USDcorpCCC3-5y',
    'J6A1':'USDcorpBB5-10y', 'EMGB':'USDemsovHY', 'IQDL':'USDlatamsovHY','IQDA':'USDasiasovHY'
    },
    'spread':{'LGCPTRUH':'USDglobalaggcorp', 'BAC1TRUU':'USDglobalaggcorp1-3y', 'LG30TRUU':'USDglobalHY'},
    'yld':{'LGCPTRUH':'USDglobalaggcorp', 'BAC1TRUU':'USDglobalaggcorp1-3y', 'LG30TRUU':'USDglobalHY'}
    }

#load BBG credit data into a dictionary of dfs
BBG_creddata = pd.read_excel(
    f'{datapath1}credit.xlsx',
    sheet_name=credit_dts,
    index_col=0,
    parse_dates=True
    )

# concatenate the dictionary into a single df
BBGcred_df = pd.concat(
    [BBG_creddata['tri'],BBG_creddata['spread'],BBG_creddata['yld']],
    keys=['tri','spread','yld'],
    names=['datatype','gen_index'], axis=1)

# change the BBGcred_df columns from BBG index names to CCR generic index names
idx = [
    (item[0], credit_label_dict[item[0]][str(item[1]).replace(' Index','')]) 
    for item in BBGcred_df.columns
    ]
BBGcred_df.columns = pd.MultiIndex.from_tuples(idx, names=['datatype', 'gen_index'])

# pickle the dataframe
BBGcred_df.to_pickle('C:/Code/asset_allocation/cred_pickles.pkl')
