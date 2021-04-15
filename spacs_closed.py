import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from functions import dgfuncs
import xlrd

# load data from xlsx
common = pd.read_excel(r'C:/data/BBG_data/closed_spacs.xlsx', sheet_name='Common', index_col=0, parse_dates=True)
hist_iv1 = pd.read_excel(r'C:/data/BBG_data/closed_spacs.xlsx', sheet_name='iv1', index_col=0, parse_dates=True)
hist_iv2 = pd.read_excel(r'C:/data/BBG_data/closed_spacs.xlsx', sheet_name='iv2', index_col=0, parse_dates=True)
key_dates = pd.read_excel(r'C:/data/BBG_data/closed_spacs.xlsx', sheet_name='key_dates', index_col=0, parse_dates=False)

# the clean DF is the common, which has share prices for each closed SPAC
# the 'dirty' DFs are those for the warrants, containing historic implied vol data (HIV) 
# BBG uses different tickers for different warrants, so I downloaded two warrant sheets (HIV1 & HIV2), one for each ticker
# each warrant sheet contains about half of the warrants of the commons in the common sheet 
# the warrant sheets have to be merged into one DF containing the historic IVs for each of the stocks in the common sheet

# to do this:
# -- determine which sheets columns are to be discarded based on nuniques as a share of length
# -- outer merge the data remaining
# -- convert the names in the warrant DF to match the names in the common df

for df in [common, hist_iv1, hist_iv2]:
    df.drop('Dates', axis=0, inplace=True) #drop cell with 'Dates' (used as a BBG column header) with a row label: 'BBG_ticker'

common = common.loc[:, ~common.loc['Name'].str.contains('#N/A')] #remove all columns with no name in the column header
hist_iv1 = hist_iv1.loc[:, ~hist_iv1.loc['Name'].str.contains('#N/A')]
hist_iv2 = hist_iv2.loc[:, ~hist_iv2.loc['Name'].str.contains('#N/A')]

#create clean dataframes for Announcement and Closed dates
# NB some Tickers don't have Announced and/or Closed dates
# ==> some dates are NaTs and the shape of Announcements and Closeds series won't be the same

announce_date = key_dates.loc['Announced', :].dropna().apply(lambda x: dgfuncs.cleandates(x))
announce_date = announce_date.astype('datetime64[ns]')

close_date = key_dates.loc['Closed', :].dropna().apply(lambda x: dgfuncs.cleandates(x))
close_date = close_date.astype('datetime64[ns]')

# remove pre-dealclose stock price data
common.drop('Name', axis=0, inplace=True) # drop the 'Name' row to leave the index with only dates
common.index = common.index.astype('datetime64[ns]') #set type as datetime64
for col in common.columns:
    pass

hist_iv2.set_index(hist_iv1.index, inplace=True) # set iv2 index to match that of iv1
hist_iv = pd.concat([hist_iv1, hist_iv2], axis=1) #concatentate iv1 and iv2 into the same iv df

#remove any duplicated tickers
#can arise when scattergunning BBG's different warrant codes if the two guessed warrant codes return different warrants for different companies
hist_iv = hist_iv.loc[:, ~hist_iv.columns.duplicated()]

# merge common and hist_iv into a single df with stock data and IV data
hist_iv.drop('Name', axis=0, inplace=True) # first drop the 'Name' row from index to allow for coming merge
closedspac_df = common.merge(hist_iv, left_index=True, right_index=True, suffixes=('_stock', '_iv'))

# the merged df now has stock and IV data, but includes stocks with no IV
# since this is no use to us, we drop the 'orphan' stock data from the merged df (ie keep only rows which have '_stock' or '_iv' in the name)
closedspac_df = closedspac_df.loc[
    :,
    closedspac_df.columns.str.contains('_stock')|
    closedspac_df.columns.str.contains('_iv')] #select only entries which have data for stock price and IV

if closedspac_df.columns.str.contains('_iv').sum() != closedspac_df.columns.str.contains('_stock').sum():
    print('WARNING: number of entries for stock and IV data do not match')

# clean up the stock price and vol dfs
stock_df = closedspac_df.loc[:, closedspac_df.columns.str.contains('_stock')]
stock_df.columns = stock_df.columns.str.replace('_stock', '')
stock_df.index = stock_df.index.astype('datetime64[ns]')

iv_df = closedspac_df.loc[:, closedspac_df.columns.str.contains('_iv')]
iv_df.columns = iv_df.columns.str.replace('_iv', '')
iv_df.index = iv_df.index.astype('datetime64[ns]')

hv_df = stock_df.pct_change().rolling(21).std().multiply(251**0.5).multiply(100)

spacs_closed_df = pd.concat([stock_df, iv_df, hv_df], keys=['stock', 'iv', 'hv'],names=['kind', 'ticker'], axis=1)
spacs_closed_df.columns = spacs_closed_df.columns.swaplevel(0, 1)
spacs_closed_df = spacs_closed_df.sort_index(axis=1)

# discard tickers where data for one of the kinds (ie stock, hv, iv) is missing

for col in spacs_closed_df.columns.get_level_values(0).drop_duplicates():
    if 0 in list(spacs_closed_df[col].nunique()):
        spacs_closed_df.drop(columns=col, level=0, inplace=True)



