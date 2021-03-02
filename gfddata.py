import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd

from functions import gfdfuncs as gfd

gfd_bond_tri_dict = {
    'TRUSG10M':'UST10y',
    'TRUSABIM':'US3m'
}

gfd_commods_dict = {
    'GFDCONATGAS':'natgas', 
    'CMTOBACM': 'tobac'
}

gfd_eq_tri_dict = {
    'GFTRJPNSTD':'Topix',
    'GFUK100MRM':'UKX',
    'GFTSWLDM':'WORLD',
    'GFUS100MRD':'SPX'
}

gfd_pct_GDP = {
    'SCWLDITMCAPPCTM':"WDtechpcGDP",
    'SCUSALLITMCAPPCTM':'UStechpcGDP',
    'SCUSALLITMCAPPCTM':"USmatspcGDP"
}

gfd_mktcap = {
    '_5SP10MCAP':'SPXmtkcap_energy',
    '_5SP45MCAP':'SPXmktcap_tech',
    '_5P5TOTMCAPM':'SPXmktcap'
}

gfd_mktcap_pc = {
    'SCUSALLITMCAPPCTM':'SPXmktcappc_tech'
}