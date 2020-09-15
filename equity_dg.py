# load & wrangle data it into a clean workable dataframe which can be pickled
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

for key in equity_data_dict.keys():

    # replace values in equity_data_dict with BBG_df object to yield a dictionary with dfs as dictvals
    equity_data_dict[key] = pd.read_excel(
    f'{datapath1}equity_data.xlsx', sheet_name=equity_data_dict[key], index_col = 0, parse_dates=True)

    #clean BBG_df columns by removing ' Index' from string 
    equity_data_dict[key].columns = [col.replace(' Index','') for col in equity_data_dict[key].columns]

    #replace BBG_df columns of BBG index names with generic index labels
    equity_data_dict[key].columns = [label_dict[key][second_key] for second_key in equity_data_dict[key].columns]
    
# combing all xls sheets into one dataframe
# hierarchical index levels are:  datatype, generic index label
BBG_df = pd.concat(
    [equity_data_dict['tri'],equity_data_dict['pi'],equity_data_dict['eps'],equity_data_dict['dps']],
    axis = 1,
    keys=['tri','pi','eps','dps']
    )

#convert monthly data to daily
BBG_df['eps']=BBG_df['eps'].fillna(method='ffill')
BBG_df['dps']=BBG_df['dps'].fillna(method='ffill')

BBG_df.columns.names = ('datatype','gen_index')

# recalculate tri's where price and dividend data allow
CCR_tri = pd.DataFrame()
for ind in BBG_df['pi'].columns:
    if ind in BBG_df['dps']:
        CCR_tri[ind] = (BBG_df['pi'][ind]+BBG_df['dps'][ind]/20.7)/BBG_df['pi'][ind].shift(1)
        CCR_tri[ind] = CCR_tri[ind].cumprod()

# overwrite BBG tri's with CCR tris


# load nonBBG into new dataframe
# returns dictionary with worksheet names for keys, pandas BBG_df for values
nonBBG_eqdata = pd.read_excel(
    'C:/Data/nonBBG_Data/historic_financial/equity_data_NBBG.xlsx',
    ['tri', 'pi', 'eps', 'dps','cape']
    )



#BBG_df.to_pickle('C:/Code/Pickles/eq_pickles.pkl')
