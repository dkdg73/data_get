# Script for pulling data from the GFD Series API
# Author: Jonathan Grundy and Robert Mohr, GFD Data Scientists
# Written in Python 2

import requests
import pandas as pd
import datetime
import os
import getpass

#GFD login function
def gfd_auth(username = None, password = None):
    """
    Pulls a GFD API token and stores it as an environmental variable.
    
    Parameters
        username: GFD-approved email address.

        password: Password for GFD-approved email address.
    """
    if username is None:
        username = getpass.getpass('Please enter your GFD Finaeon username: ')

    if password is None:
        password = getpass.getpass('Please enter your GFD Finaeon password: ')

    url = 'https://api.globalfinancialdata.com/login/'
    parameters = {'username': username, 'password': password}
    resp = requests.post(url, data = parameters)

    #check for unsuccessful API returns
    if resp.status_code != 200:
        raise ValueError('GFD API request failed with HTTP status code %s' % resp.status_code)

    json_content = resp.json()
    os.environ['GFD_API_TOKEN'] = json_content['token'].strip('"')
    #print("GFD API token recieved at %s" % str(datetime.datetime.now()))

# call the gfd_auth function with credentials
gfd_auth(username='dg@calderwoodcapital.com', password='l0vemeGFD!!')

# url for accessing the GFD series API
url = 'https://api.globalfinancialdata.com/series'

# parameters list for series API
parameters = {
    'token': os.environ['GFD_API_TOKEN'],
    'seriesname': 'TRUSG10M'
    }

#API call return body
r = requests.post(url, data = parameters)

#extract the price data; assign it to a pandas dataframe
data = pd.DataFrame(r.json()['price_data'])
data = data[['series_id', 'date', 'open', 'high', 'low', 'close', 'openint', 'volume']]

