# load & wrangle BBG govt bond index data into a clean workable dataframe for pickling
# takes bloomberg data for contemporary data
# takes BAML data for majority of index attributes (eg yld, duration, spreads)
# historical data from various sources for history
# splices series together where possible to give the longest run of data possible


import numpy as np
import pandas as pd
import statistics
import matplotlib.pyplot as plt
import xlrd

from functions import dgfuncs as funcs

datapath1 = r'C:/data/BBG_data/'
datapath2 = r'C:/data/nonBBG_data/'

bond_dts = ['tri', 'yld', 'yld_bid', 'yld_ask']

bond_label_dict={
    'tri':{'G1O2 Index':'USbonds1-3y',	'G2O2 Index':'USbonds3-5y', 'LUATTRUU Index':'USbonds7-10y',
    'G8O2 Index':'USbonds15y+', 'LGAGTRUH Index':'WDbondagg', 'EG01 Index':'EURbonds1-3y',
    'EG02 Index':'EURbonds3-5y','EG04 Index':'EURbonds7-10y','EG08 Index':'EURbonds15y+'},
    'yld':{'LGAGTRUH Index':'WDbondagg'},
    'yld_bid':{'USGG2YR Index':'USbond2y', 'GT5 Govt':'USbond5y','USGG10YR Index':'USbond10y',
    'USGG30YR Index':'USbond30y'}, 
    'yld_ask':{'USGG2YR Index':'USbond2y', 'GT5 Govt':'USbond5y','USGG10YR Index':'USbond10y',
    'USGG30YR Index':'USbond30y'}
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
    [BBG_bonddata['tri'],BBG_bonddata['yld'], BBG_bonddata['yld_bid'], BBG_bonddata['yld_ask']],
    keys=['tri','yld', 'yld_bid', 'yld_ask'],
    names=['datatype','gen_index'], axis=1)

# change the BBGbond_df columns from BBGto CCR generic index names
idx = [(item[0], bond_label_dict[item[0]][item[1]]) for item in BBGbond_df.columns]
BBGbond_df.columns = pd.MultiIndex.from_tuples(idx, names=['datatype', 'gen_index'])

# calculate midpoint_ylddf, containing the mid point bond yields of yld_bid and yld_ask
mid = lambda x, y: (x + y) / 2
midpoint_ylddf = BBGbond_df['yld_bid'].combine(BBGbond_df['yld_ask'], mid)

#add the midpoint_ylddf into the main BBGbond_df
for col in midpoint_ylddf.columns:
    BBGbond_df['yld', col] = midpoint_ylddf[col]

BBGbond_df.sort_index(axis=1, inplace=True)


###########################################################
### THIS COMPLETES THE CODE FOR THE LOADING OF BBG DATA ###
###########################################################

#load nonBBG data
nonBBG_bonddata = pd.read_excel(
    'C:/Data/nonBBG_Data/historic_financial/bond_data_NBBG.xlsx',
    ['tri', 'yld'],
    index_col=0,
    parse_dates=True
    )

# concatenate the pandas excel dictionary of worksheet dfs to a single df
nonBBGbond_df = pd.concat(
    [nonBBG_bonddata['tri'],nonBBG_bonddata['yld']],
    keys=['tri','yld'],names=['datatype','gen_index'], axis=1)

# set date range to the end of the month (couldn't figure out how to default to end month)
nonBBGbond_df.index = pd.date_range(
    start='1871-01',periods=nonBBGbond_df['tri']['USbonds7-10y'].count(),freq='M'
    )

# resample the data to busines daily data, filling forward the NaNs
nonBBGbond_df = nonBBGbond_df.resample('B').last().fillna(method='ffill', limit=35)

# non_BBG data is lapsed and doesn't update 
# roll forward the date index to bring it uptodate, and ensure merge compatibility
last_nonBBG_datapoint = nonBBGbond_df.index[-1].date().isoformat()
last_BBG_datapoint = BBGbond_df.index[-1].date().isoformat()

dates = pd.date_range(start=last_nonBBG_datapoint, end=last_BBG_datapoint,freq='B')
nonBBGbond_df=nonBBGbond_df.append(pd.DataFrame(index=dates))


# combine nonBBG df with BBG df by creating a dictionary of spliced series to concatenate
datatype_list=['tri','yld']
datatype_dict={}

for dt in datatype_list:
    datatype_dict[dt]=funcs.splice_df(nonBBGbond_df[dt], BBGbond_df[dt])

#concatenate the dictionary values into a single df
bond_df=pd.concat(
    [datatype_dict['tri'],datatype_dict['yld']],
    axis=1, keys=datatype_list, names=['datatype', 'gen_index']
    )

conc = pd.concat([BBGbond_df['yld_bid'], BBGbond_df['yld_ask']], axis =1, keys=['yld_bid', 'yld_ask'])
bond_df = pd.concat([bond_df, conc], axis=1)

bond_df.to_pickle('C:/Data/pickles/bond_pickles.pkl')
