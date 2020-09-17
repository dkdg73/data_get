# load & wrangle equity index data into a clean workable dataframe for pickling
# takes bloomberg data for contemporary data
# historical data from various sources for history
# splices series together where possible to give the longest run of data possible
# also import chart_funcs module so that charts of the series can be run from the interactive window

import funcs

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd

datapath1 = r'C:/data/BBG_data/'
datapath2 = r'C:/data/nonBBG_data/'

# create a dictionary mapping datakind labels (keys) to datasheet names (values)
equity_data_dict = {
    'tri':'total_return_index', 'pi':'price_index', 'eps':'eps','dps':'dps'
    }

# nested dictionary mapping datakind label keys to dictionaries mapping BBG index names to generic index labels
label_dict = {
    'tri': {'SPXT': 'spx', 'XCMP':'ndxcomp', 'XNBI':'biotech', 
        'M1WD':'acwi', 'M1EF':'em','RU30INTR':'rus3000',
        'RU20INTR':'rus2000'},
    'pi': {'SPX': 'spx', 'NDX':'ndx100', 'CCMP':'ndxcomp','MXWD':'acwi','MXEF':'em','RTY':'rus2000','RAY':'rus3000'},
    'eps': {'SPX': 'spx','NDX':'ndx100', 'CCMP':'ndxcomp','MXWD':'acwi','MXEF':'em','RTY':'rus2000','RAY':'rus3000'},
    'dps': {'SPX': 'spx','NDX':'ndx100', 'CCMP':'ndxcomp','MXWD':'acwi','MXEF':'em','RTY':'rus2000','RAY':'rus3000'}
    }

#load BBG data
for key in equity_data_dict.keys():

    # replace values in equity_data_dict with BBG_df object to yield a dictionary with dfs as dictvals
    equity_data_dict[key] = pd.read_excel(
    f'{datapath1}equity_data.xlsx', sheet_name=equity_data_dict[key], index_col = 0, parse_dates=True)

    #clean BBG_df columns by removing ' Index' from string 
    equity_data_dict[key].columns = [col.replace(' Index','') for col in equity_data_dict[key].columns]

    #replace BBG_df columns of BBG index names with generic index labels
    equity_data_dict[key].columns = [label_dict[key][second_key] for second_key in equity_data_dict[key].columns]
    
# combing all xls sheets into one dataframe
# keys parameter creates a hierarchical index 
# teh heirarchical index level names are:  datatype, generic index label
BBG_df = pd.concat(
    [equity_data_dict['tri'],equity_data_dict['pi'],equity_data_dict['eps'],equity_data_dict['dps']],
    axis = 1,
    keys=['tri','pi','eps','dps'],
    names=['datatype', 'gen_index']
    )

#convert monthly eps & dps data to daily
BBG_df['eps']=BBG_df['eps'].fillna(method='ffill', limit = 30)
BBG_df['dps']=BBG_df['dps'].fillna(method='ffill', limit = 30)

# recalculate tri's where price and dividend data allow
# tri_calc takes daily price and monthly div data paddded to daily frequency 
# hence divide div by 20.7to calculate daily div return 
tri_calc = lambda x, y: (x + y/20.7)/x.shift(1)
dCCR_df = BBG_df['pi'].combine(BBG_df['dps'], tri_calc)

# reset the index to be heriarchical in the same way as BBG_df, (using a neat self-concat)
dCCR_df = pd.concat([dCCR_df], keys=['tri'], names=['datatype'], axis=1)
CCR_df = dCCR_df.cumprod()

#splice longer CCR tris with shorter BBG tris
new_df = funcs.splice_df(CCR_df['tri'],BBG_df['tri'])
CCR_df = pd.concat([new_df], keys=['tri'], names=['datatype'], axis=1)
 

# create a new df with CCR-created tris instead of BBG-provided tris
cleanBBG_df = CCR_df.combine_first(BBG_df)

###########################################################
### THIS COMPLETES THE CODE FOR THE LOADING OF BBG DATA ###
###########################################################

# load nonBBG equity data
nonBBG_eqdata = pd.read_excel(
    'C:/Data/nonBBG_Data/historic_financial/equity_data_NBBG.xlsx',
    ['tri', 'pi', 'eps', 'dps','cape'],
    index_col=0,
    parse_dates=True
    )

# concatenate the pandas excel dictionary of worksheet dfs to a single df
nonBBG_df = pd.concat(
    [nonBBG_eqdata['tri'],nonBBG_eqdata['pi'],nonBBG_eqdata['eps'],nonBBG_eqdata['dps'],nonBBG_eqdata['cape']],
    keys=['tri','pi','eps','dps','cape'],
    names=['datatype','gen_index'], axis=1)

# set date range to the end of the month (couldn't figure out how to default to end month)
nonBBG_df.index = pd.date_range(
    start='1871-01',periods=nonBBG_df['tri']['spx'].count(),freq='M'
    )

# non_BBG data is lapsed and doesn't update 
# roll forward the date index to bring it uptodate, and ensure merge compatibility 
dates = pd.date_range(start='2019-10', periods=12,freq='M')
nonBBG_df=nonBBG_df.append(pd.DataFrame(index=dates))


# resample the data to daily data, filling forward the NaNs
# resample again to business daily data 
# !! resampling biz daily from the start leads to missing data because !!
# !! the monthly data is end calendar month, which is not necessarily a business day!!
nonBBG_df = nonBBG_df.resample('D').asfreq().fillna(method='ffill', limit=35)
nonBBG_df = nonBBG_df.resample('B').asfreq()


# combine nonBBG df with cleanBBG_df by creating a dictionary of spliced series to concatenate
datatype_list=['tri','pi','eps','dps']
datatype_dict={}

for dt in datatype_list:
    datatype_dict[dt]=funcs.splice_df(nonBBG_df[dt], cleanBBG_df[dt])

#concatenate the series
eq_df=pd.concat(
    [datatype_dict['tri'],datatype_dict['pi'], datatype_dict['eps'],datatype_dict['dps']],
    axis=1, keys=datatype_list, names=['datatype', 'gen_index']
    )

#################################################################################
### THIS COMPLETES THE LOADING AND SPLICING OF HISTORICAL & CONTEMPORARY DATA ###
#################################################################################

# pickle the dataframe
eq_df.to_pickle('C:/Code/asset_allocation/eq_pickles.pkl')

