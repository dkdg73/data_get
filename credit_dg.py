# load & wrangle BBG credit index data into a clean workable dataframe for pickling
# takes bloomberg data for contemporary data
# takes BAML data for majority of credit index attributes (eg yld, duration, spread)
# historical data from various sources for history
# splices series together where possible to give the longest run of data possible

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import xlrd

import dgfuncs as funcs

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
    names=['datatype','gen_index'],
    axis=1
    )

# change the BBGcred_df columns from BBG index names to CCR generic index names
idx = [
    (item[0], credit_label_dict[item[0]][str(item[1]).replace(' Index','')]) 
    for item in BBGcred_df.columns
    ]
BBGcred_df.columns = pd.MultiIndex.from_tuples(idx, names=['datatype', 'gen_index'])
BBGcred_df.index.name = 'Date'

###########################################################
### THIS COMPLETES THE CODE FOR THE LOADING OF BBG DATA ###
###########################################################

# the nonBBG credit data comes from various sources
# the historic data in credit_NBBG.xlsx comes from FRED and GFD
# -- AAA total returns come from an old GFD file (1871-2013, MONTHLY)
# -- AAA and BAA yields come from a FRED download (1919-2020, MONTHLY AVERAGES)
# -- the old GFD dates are a mess (just years, no months)
# -- the FRED yield data is monthly average, with each month stamped as the FIRST day of the month
# the BAML-ICE yld data only starts in 1984, so misses out on what happens in the 1970s, hence need to splice with FRED
# -- 5-10y A-BBB yields come from a BAML-ICE download (C6C0, 1984-2020, DAILY CLOSE)
# -- the BAML ICE dates are a FUCKING mess: mainly with date/month inconsistencies which confuse the date parser 
# -- but also doesn't include holidays in its date index
# -- this needs to be cleaned before splicing or merging with other datasets
# the following code does the following 
# -- cleans up historical data in each worksheet in the historic data xlsx to prepare it for merging and splicing
# -- merges each sheet's data into one dataframe for historic data
# -- cleans up the BAML-ICE data (ie its erroneously formatted dates)
# -- the resulting BAML-ICE ylds dataframe is then spliced with the historic ylds dataframe to yield a longer run of yld data
# merges this non-BBG df with the BBG df, and pickles it

# load historic credit data 
# NB/ the historic data is from GFD and FRED
HIST_creddata=pd.read_excel(
    f'{datapath2}historic_financial/credit_NBBG.xlsx',
    sheet_name=['tri','yld'],
    index_col=0,
    parse_dates=True
    )

# start with the 'tri' sheet
# reindex to the earliest starting point
# create an empty df with the desired date index
dates = pd.date_range(start='1871-02', end='2020-10',freq='M')
df = pd.DataFrame(index=dates)
df.index.name = 'Date'

# clean the historic tri data's dates by replacing with a pd.date range and naming it properly
HIST_creddata['tri'].index = pd.date_range(start='1871-02', periods= HIST_creddata['tri'].shape[0], freq='M')
HIST_creddata['tri'].index.name = 'Date'

# merge the historic tri data (which stops in 2013) into the empty df (which runs up to the present)
HIST_creddata['tri'] = pd.merge(df, HIST_creddata['tri'], how='left', on='Date')

# resample historic tri data into daily business frequency
HIST_creddata['tri'] = HIST_creddata['tri'].resample('D').asfreq().fillna(method='ffill', limit=35)
HIST_creddata['tri'] = HIST_creddata['tri'].resample('B').asfreq()

# Load ICE yield data for C6C0
# NB1 at time of coding, the the ICE xlsx was one worksheet and one dataseries so it 
# automatically so the pd.read_excel call automatically returns a dataframe
# NB2 there is quite a nasty corruption of the date index in the spreadsheet
# it mixes text with dates, which causes the pandas date parser to mix up certain months
# the text is month first format, so the parser uses that when parsing the rest of the date
# but when it finds date formats, it assumes day first. so 1st-Jun becomes 6th-Jan
# obviouisly, this then screws up the sequencing of the data for charts etc 
# I thought about staying with monthly as short cut, but some of the month ends were messed up too
# I couldn't figure out a clean way to fix the problem other than to format the dates myself 
# therefore, indexing and date parsing was skipped during the pd.read_excel 
# dates borought in as an unindexed series
# the unformatted date is run through a list comprehension which returns Boolean according to 
# whether or note the date format recieved by pd is in string format
# the uses that Boolean to run another list comprehension to correct the date formatting error
# list comprehensions aren't the idea way to fix this but I couldn't figure out how to do it vectorized

# load the ICE data
ICE_creddf=pd.read_excel(
    f'{datapath2}ICE_BAML/ice_credit.xlsx',
    parse_dates=False
    )

# create Boolean column for the Date column's datatype
date_type_str = [type(ICE_creddf['Date'].iloc[i])==str for i in range(ICE_creddf.shape[0])]
ICE_creddf['date_type'] = date_type_str

# if Datatype isn't str (ie 'date_type'==False), we want to reverse its days and months
# first create the date inverting function to be used
# invd function takes datetime dtype and switches month with day
def invd(d):
    return datetime.date(year=d.year, month=d.day, day=d.month)

# now convert the entire Date column to datetime format so the invd function works on each element
ICE_creddf['Date']=pd.to_datetime(ICE_creddf['Date'])

# create a 'new_Date' series, correcting the incorectely formatted dates using the invd function
ICE_creddf['new_Date']=[
    ICE_creddf['Date'].iloc[i]
    if ICE_creddf['date_type'].iloc[i] 
    else invd(ICE_creddf['Date'].iloc[i]) 
    for i in range(ICE_creddf.shape[0])
    ]

# set index to correct date
ICE_creddf = ICE_creddf.set_index(ICE_creddf['new_Date'])

# remove columns used to do the calculations; name the index
ICE_creddf = ICE_creddf.drop(columns=['Date','date_type','new_Date'])
ICE_creddf.index.name = 'Date'

# splice C6C0 with the Moody's BAA series
# NB1/that the Moody's data is averaged monthly
# NB2/that the Moody's AAA and BAA series are 20y+ maturity vs BAML-ICE 5-10yr
# ==> we are going to splice Moody's BAA yield with ICEBAML
 
# convert the FRED yld data's index to a month-end index, so the yld df can merge with the tri df
HIST_creddata['yld'].index = HIST_creddata['yld'].index.to_period('M').to_timestamp('M')

# convert the FRED yld data to business day frequency 
HIST_creddata['yld'] = HIST_creddata['yld'].resample('D').asfreq().fillna(method='bfill', limit=35)
HIST_creddata['yld'] = HIST_creddata['yld'].resample('B').asfreq()

# merge the yield data from FRED data with the yld data from ICE 
mrgd_yld_data = pd.merge(HIST_creddata['yld'], ICE_creddf, how='left', left_on='Date', right_on='Date')
mrgd_yld_data.ffill(inplace=True)

# create an adjusted C6C0 series based on the BAA series
mrgd_yld_data['adjC6C0']=mrgd_yld_data['BAA']-0.5

# splice the adjusted C6C0 series with original C6C0 to create a long run of history
mrgd_yld_data['spl_BAA_C6C0']=mrgd_yld_data['C6C0'].combine_first(mrgd_yld_data['adjC6C0'])
mrgd_yld_data.drop(columns=['adjC6C0','C6C0'], inplace=True)

# the next section concatenates the yld and tri into a single two-leveled df
# the first layer will be used as dictionary keys when we splice the BBG data with the non_BBG data, which we then reconcatenate again

nonBBGcred_df=pd.concat(
    [HIST_creddata['tri'], mrgd_yld_data],
    axis=1, keys=['tri', 'yld'], names=['datatype', 'gen_index']
    )

# clean up the column names of the new concatenated df
# NB the nonBBG cred label dict uses the spliced_BAA_C6C0 only, the shorter ICE data is discarded 
nonBBGcred_label_dict = {
    'tri':{'AAACorp': 'USDcorpAAA20y+'},
    'yld':{'AAA': 'USDcorpAAA20y+', 'BAA':'USDcorpABB20y+', 'spl_BAA_C6C0':'USDcorpA-BBB5-10y'}
}

# change the BBGcred_df columns from BBG index names to CCR generic index names
#idx = [
#    (item[0], credit_label_dict[item[0]][str(item[1]).replace(' Index','')]) 
#    for item in BBGcred_df.columns
#    ]

#return a list of tuples representing first level, second level (ie datatype, gen_index) using CCR names
idx = [(item[0], nonBBGcred_label_dict[item[0]][item[1]]) for item in nonBBGcred_df.columns]

# use the newly created tuple list for the series column names
nonBBGcred_df.columns = pd.MultiIndex.from_tuples(idx, names=['datatype', 'gen_index'])

# concatenate the
cred_df = pd.concat([nonBBGcred_df, BBGcred_df], axis=1)

# pickle the dataframe
cred_df.to_pickle('C:/Code/asset_allocation/pickles/cred_pickles.pkl')
