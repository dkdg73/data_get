
### DEBUGGING FUNCTIONS ###

import pandas as pd
import numpy as np

# returns list with series, startdate for each variable
# takes single layer df as input
def get_start_dates(df):
    lsta = {}
    for ind in df.columns:
        lsta[ind] = df[ind].first_valid_index().date().isoformat()
    return lsta

# returns a dictionary of indices and their corresponding cagrs 
# takes a single layer df as input 
def get_cagrs(df):
    for ind in df.columns:
        start = df[ind].first_valid_index().date().isoformat()
        end = df[ind].last_valid_index().date().isoformat()
        cagr = (df[ind].loc[end]/df[ind].loc[start])**(260.7/df[ind].count())
        print(f'{ind} cagr:', f'{round(cagr,2)} ({start} to {end})', sep='\t')

# returns the max value and its date for indices in a df
# takes a single layer df as input
def get_max_details(df):
    for ind in df:
        max_date = df[ind].idxmax(axis=1).date().isoformat()
        max_val = df[ind].loc[max_date]
        print(ind, max_date, round(max_val,2), sep='\t')

# returns the max value and its date for indices in a df
# takes a single layer df as input
def get_min_details(df):
    for ind in df:
        min_date = df[ind].idxmin(axis=1).date().isoformat()
        min_val = df[ind].loc[min_date]
        print(ind, min_date, round(min_val,2), sep='\t')

# returns a single series spiced from two other series
# the discontinuity at the join is smoothed by rebasing the longer time series
# to the shorter time series at the join 
# takes two pd series as input
def splice_s(x, y):
    # find the date to rebase and join
    x_start = x.first_valid_index().date().isoformat()
    y_start = y.first_valid_index().date().isoformat()
    splice_date = max(x_start, y_start)
    # find the shortest index (ie the index with empty cells to fill)
    # !!note the use of count() instead of nunique()!!
    # !!nunique will understate the size of the dataset for padded values!!
    if x.count() < y.count():
        splice_factor = x.loc[splice_date]/y.loc[splice_date]
        # fill the emtpy cells with newly rebased data from the longer series
        spliced = x.fillna(splice_factor*y)
    else:
        splice_factor = y.loc[splice_date]/x.loc[splice_date]
        spliced = y.fillna(splice_factor*x)

    return spliced

# returns a single df from two dfs with overlapping series spliced 
# into the longest possible series
# the dfs inputed must be single layered
def splice_df(x, y):
    # first identify which df is the longest, so as to correctly merge on its index
    if x.index.nunique() > y.index.nunique():
        toggle = 'left'
    else:
        toggle = 'right'
    
    # merge the two dfs on the datetime index
    merged_df = pd.merge(x, y, how=toggle, left_index=True, right_index=True)
    # create a list of the names of market indices to be spliced
    mkts = [mkt.replace('_x','') for mkt in [col for col in merged_df if '_x' in col]]
    # add the spliced market indices to the merged df
    for mkt in mkts:
        merged_df[mkt]=splice_s(merged_df[f'{mkt}_x'],merged_df[f'{mkt}_y'])
    # remove the suffixed data from the merged df
    merged_df = merged_df[[col for col in merged_df.columns if not'_y' in col]]
    merged_df = merged_df[[col for col in merged_df.columns if not'_x' in col]]

    return merged_df

# returns the currency return for a USD investor buying FX and leaving on deposit unhedged
# takes USD based FX pair and the FX 3m rate as pandas df or series
def get_fxtri(x, y):
    daily_currency_return = x.offset(1)/x
    daily_interest_income = y.div(100).add(1).pow(1/90).sub(1)
    daily_return = daily_currency_return.add(daily_interest_income)
    return daily_return.cumprod()



    
