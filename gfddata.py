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
    'CMTOBACM', 'tobac'
}

gfd_eq_tri_dict = {
    'GFTRJPNSTD':'Topix';
    'GFUK100MRM':'UKX';
    'GFTSWLDM';'WORLD';
    'GFUS100MRD';'SPX'
}