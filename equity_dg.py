# load in the data & wrangle it into a clean workable dataframe
# also import chart_funcs module so that charts can be run from the interactive window

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd

datapath = r'C:/data/BBG_data/'

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

    # replace values in equity_data_dict with df object to yield a dictionary with dfs as dictvals
    equity_data_dict[key] = pd.read_excel(
    f'{datapath}equity_data.xlsx', sheet_name=equity_data_dict[key], index_col = 0, parse_dates=True)

    #clean df columns by removing ' Index' from string 
    equity_data_dict[key].columns = [col.replace(' Index','') for col in equity_data_dict[key].columns]

    #replace df columns of BBG index names with generic index labels
    equity_data_dict[key].columns = [label_dict[key][second_key] for second_key in equity_data_dict[key].columns]
    
# combing all xls sheets into one dataframe
# hierarchical index levels are:  datatype, generic index label
df = pd.concat(
    [equity_data_dict['tri'],equity_data_dict['pi'],equity_data_dict['eps'],equity_data_dict['dps']],
    axis = 1,
    keys=['tri','pi','eps','dps']
    )

#convert monthly data to daily
df['eps']=df['eps'].fillna(method='ffill')
df['dps']=df['dps'].fillna(method='ffill')

df.columns.names = ('datatype','gen_index')

df.to_pickle('C:/Code/Pickles/eq_pickles.pkl')
