# load & wrangle BBG fx, commod, crypto data into a clean workable dataframe for pickling
# takes bloomberg data for contemporary data
# historical data from various sources for history
# splices series together where possible to give the longest run of data possible

#import dgfuncs

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd

datapath1 = r'C:/data/BBG_data/'
datapath2 = r'C:/data/nonBBG_data/'

money_dts = ['fx', '3m_bills','3m_basis','commods','crypto']
names=['datatype','underlying']

money_label_dict={
    'fx':{'USDCHF Curncy':'USDCHF'},
    '3m_bills':{'USGG3M Index':'USmoney3m','SFDRC Curncy':'CHmoney3m'},
    '3m_basis':{'CHF3M BGN Curncy':'USDCHGbasis3m'},
    'commods':{'XAU Curncy':'gold',	'XAG Curncy':'silver'},
    'crypto':{'XBTUSD Curncy':'bitcoin','XETUSD Curncy':'ether'}
    }

#load BBG credit data into a dictionary of dfs
BBGmoney_data = pd.read_excel(
    f'{datapath1}money.xlsx',
    sheet_name=money_dts,
    index_col=0,
    parse_dates=True
    )

# concatenate the dictionary into a single df

BBGmoney_df = pd.concat(
    [BBGmoney_data['fx'],BBGmoney_data['3m_bills'],BBGmoney_data['3m_basis'],
    BBGmoney_data['commods'],BBGmoney_data['crypto']],
    keys=money_dts,
    names=names,
    axis=1
    )

# change the BBGmoney_df columns from BBGto CCR generic index names
idx = [(item[0], money_label_dict[item[0]][item[1]]) for item in BBGmoney_df.columns]
BBGmoney_df.columns = pd.MultiIndex.from_tuples(idx, names=names)

BBGmoney_df.to_pickle('C:/Code/asset_allocation/pickles/money_pickles.pkl')

# NB currency return for a usd investor is [(1/usd)/pct_chg() + r*]

#newdf = dgfuncs.get_fxtri(BBGmoney_df['USDCHF'], BBGmoney_df['3m_bills']['CHmoney3m'])
