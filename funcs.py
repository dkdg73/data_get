
# takes single layer df as input
# returns list with series, startdate for each variable
def get_start_dates(df):
    lsta = []
    for ind in df.columns:
        lsta.append([ind, df[ind].first_valid_index().date().isoformat()])
    return lsta

# takes hierarchical df as input
# returns list with datatype, series, startdate for each variable
def get_start_dates_2(df):
    for key in df.keys():
        lsta = [[key, ind, df[key][ind].first_valid_index().date().isoformat()] for key, ind in df]
    return lsta

# takes a single layer df as input 
# returns a dictionary of indices and their corresponding cagrs 
def calculate_cagrs(df):
    lsta = {}
    for ind in df.columns:
        start = df[ind].first_valid_index().date().isoformat()
        end = df[ind].last_valid_index().date().isoformat()
        lsta[ind] = (df[ind].loc[end]/df[ind].loc[start])**(260.7/df[ind].count())
    return lsta