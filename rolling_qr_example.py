# -*- coding: utf-8 -*-

import sys
import pandas as pd
PYTHON_FOLDER = 'C:\\Users\\dksmi\\Dropbox (TheVentureCity)\\David\\Python Scripts\\' # Change this to match your environment
sys.path.append(PYTHON_FOLDER)
import growth_accounting as ga
from datetime import datetime

pd.set_option('display.max_columns', 500)

company_name = 'SampleCo'
folder = 'C:\\Users\\dksmi\\Dropbox (TheVentureCity)\\David\\' + company_name + '\\'  # Change this to match your environment
filename = folder + 'sample_transactions.csv'

t = pd.read_csv(filename)

t.head()

dau = ga.create_dau_df(t, 
                       user_id = 'user_id', 
                       activity_date = 'dt', 
                       inc_amt = 'inc_amt')
dau.head()

dau_decorated = ga.create_dau_decorated_df(dau, use_segment = False)
dau_decorated.head()

single_window_df = ga.calc_ga_for_window(dau_decorated, datetime(2018, 6, 30).date(), 
                               window_days = 28, 
                               use_segment = False)
single_window_df.head()

window_day_sizes = [7, 28, 84]
rolling_df_no_segment = pd.DataFrame()
for w in window_day_sizes:
    this_rolling_df = ga.calc_rolling_qr_window(dau_decorated, window_days = w, use_segment = False)
    rolling_df_no_segment = rolling_df_no_segment.append(this_rolling_df)
rolling_df_no_segment.to_csv(folder + 'rolling_qr_multiwindow.csv', index = False)
rolling_df_no_segment.head()

