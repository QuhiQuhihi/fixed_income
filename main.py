import os
import numpy as np
import pandas as pd

print("hello world")

import data_loading as data


today = '2022-01-04'

# BASE_DIR = os.getcwd()
# DATA_DIR = os.path.join(BASE_DIR, 'data')
# RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw_data')
# CLN_DATA_DIR = os.path.join(DATA_DIR, 'clean_data')

ishares = data.bond_etf_data_daily(today)

ishares.ishares_daily_operation()
