# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 22:43:50 2018

@author: dksmi
"""

import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import math

### For discrete time period calculations, this helps set the variable names
### in the different dataframes 
def get_time_period_dict(time_period):
    
    time_fields_dict = {
                        'week' : {'grouping_col' : 'Week',
                                  'first_period_col' : 'first_week',
                                  'frequency' : 'Weekly',
                                  'unit' : 'Week',
                                  'period_abbr' : 'W',
                                  'python_period' : 'weeks'
                                  },
                        'month' : {'grouping_col' : 'Month_Year',
                                   'first_period_col' : 'first_month',
                                   'frequency' : 'Monthly',
                                   'unit' : 'Month',
                                   'period_abbr' : 'M',
                                   'python_period' : 'months'
                                  }
                        }
                    
    if time_period in time_fields_dict:
        time_fields = time_fields_dict[time_period]
    else:
        time_fields = None
    
    return time_fields



### Create Daily Active Users dataframe that aggregates all activity by user and day
### If the segmentation column is specified, this function includes the segment
### in the finial dataframe
def create_dau_df(transactions, 
                  user_id = 'user_id', 
                  activity_date = 'activity_date', 
                  inc_amt = 'inc_amt', 
                  segment_col = None):
    
    transactions[activity_date] = pd.to_datetime(transactions[activity_date]).dt.date
    
    if segment_col is None:
        dau = transactions.loc[transactions[inc_amt] > 0]\
                .groupby([user_id, activity_date], as_index = False)[inc_amt]\
                .sum()\
                .rename(columns = {user_id : 'user_id', 
                                   activity_date : 'activity_date', 
                                   inc_amt : 'inc_amt'})
    else:
        dau = transactions.loc[transactions[inc_amt] > 0]\
                .groupby([user_id, activity_date, segment_col], as_index = False)[inc_amt]\
                .sum()\
                .rename(columns = {user_id : 'user_id', 
                                   activity_date : 'activity_date', 
                                   inc_amt : 'inc_amt', 
                                   segment_col : 'segment'})
        
    return dau



### Using the DAU dataframe created in the function above, this creates a
### Weekly Active Users (WAU) dataframe
def create_wau_df(dau_df):
    print('Creating WAU dataframe')
    dau = dau_df.copy()
    dau['Week'] = pd.to_datetime(dau['activity_date']).dt.to_period('W')
    wau = dau.groupby(['user_id', 'Week'], as_index = False)['inc_amt']\
            .sum()
    return wau




### Using the DAU dataframe created in the function above, this creates a
### Monthly Active Users (MAU) dataframe
def create_mau_df(dau_df):
    print('Creating MAU dataframe')
    dau = dau_df.copy()
    dau['Month_Year'] = pd.to_datetime(dau['activity_date']).dt.to_period('M')
    mau = dau.groupby(['user_id', 'Month_Year'], as_index = False)['inc_amt']\
            .sum()
    return mau




### Using the DAU dataframe created in the function above, this creates a
### dataframe that contains the first usage day, week, and month for each user
def create_first_dt_df(dau_df):
    print('Creating first_dt dataframe')
    dau = dau_df.copy()
    first_dt = dau.groupby(['user_id'], as_index = False)['activity_date']\
            .min()\
            .rename(columns = { 'activity_date' : 'first_dt' })
    first_dt['first_dt'] = pd.to_datetime(first_dt['first_dt']).dt.date
    first_dt['first_week'] = pd.to_datetime(first_dt['first_dt']).dt.to_period('W')
    first_dt['first_month'] = pd.to_datetime(first_dt['first_dt']).dt.to_period('M')
    return first_dt



### Using the DAU dataframe created in the function above, this calls
### create_first_df and then merges it with the DAU dataframe to add the user's
### first usage date to the DAU dataframe
### dau_decorated is used in the subsequent functions below
### Using the segmented column from a segmented DAU dataframe is a T|F option
def create_dau_decorated_df(dau_df, use_segment, first_dt_df = None):
    print('Creating DAU Decorated dataframe')
    if first_dt_df is None:
        first_dt_df = create_first_dt_df(dau_df)
    dau_decorated_df = pd.merge(dau_df, first_dt_df, how = 'left', on = 'user_id')
    if use_segment:
        return_df = dau_decorated_df[['user_id', 'activity_date', 'inc_amt', 'segment', 'first_dt']]
    else:
        return_df = dau_decorated_df[['user_id', 'activity_date', 'inc_amt', 'first_dt']]
    return return_df



### Merging the WAU and first_dt dataframes created in the functions above, this 
### adds the user's first week to the WAU dataframe
### wau_decorated is used in the subsequent functions below
### Using the segmented column from a segmented DAU dataframe is not an option at this time
def create_wau_decorated_df(wau_df, first_dt_df):
    print('Creating WAU Decorated dataframe')
    wau_decorated = pd.merge(wau_df.copy(), first_dt_df.copy(), how = 'left', on = 'user_id')
    wau_decorated['start_of_next_week'] = pd.to_datetime(pd.PeriodIndex(wau_decorated['Week']).start_time + timedelta(weeks = 1))
    wau_decorated['Next_Week'] = wau_decorated['start_of_next_week'].dt.to_period('W')
    wau_decorated = wau_decorated[['Week', 'user_id', 'inc_amt', 'first_week', 'Next_Week']]
    return wau_decorated




### Merging the MAU and first_dt dataframes created in the functions above, this 
### adds the user's first month to the MAU dataframe
### mau_decorated is used in the subsequent functions below
### Using the segmented column from a segmented DAU dataframe is not an option at this time
def create_mau_decorated_df(mau_df, first_dt_df):
    print('Creating MAU Decorated dataframe')
    mau_decorated = pd.merge(mau_df.copy(), first_dt_df.copy(), how = 'left', on = 'user_id')
    mau_decorated['start_of_next_month'] = pd.to_datetime(pd.PeriodIndex(mau_decorated['Month_Year'], freq = 'M').start_time) + pd.DateOffset(months = 1)
    mau_decorated['Next_Month_Year'] = mau_decorated['start_of_next_month'].dt.to_period('M')
    mau_decorated = mau_decorated[['Month_Year', 'user_id', 'inc_amt', 'first_month', 'Next_Month_Year']]
    return mau_decorated



### This takes one to many rows of growth accounting figures for a specific
### date and calculates the user quick ratio
### It is used when calculating rolling quick ratio
def calc_user_qr(row, new_col = 'new', res_col = 'resurrected', churned_col = 'churned'):
    new_users = row[new_col] if hasattr(row, new_col) and pd.notnull(row[new_col]) else 0
    res_users = row[res_col] if hasattr(row, res_col) and pd.notnull(row[res_col]) else 0
    churned_users = row[churned_col] if hasattr(row, churned_col) and pd.notnull(row[churned_col]) else 0
    if churned_users < 0:
        user_qr = -1 * (new_users + res_users) / churned_users
    else:
        user_qr = math.nan
    return user_qr




### This takes one or more rows of growth accounting figures for a specific
### date and calculates the user quick ratio
### It is used when calculating rolling quick ratio
def calc_rev_qr(row, new_col = 'new', res_col = 'resurrected', 
                churned_col = 'churned', exp_col = 'expansion', 
                con_col = 'contraction'):
    new_rev = row[new_col] if hasattr(row, new_col) and pd.notnull(row[new_col]) else 0
    res_rev = row[res_col] if hasattr(row, res_col) and pd.notnull(row[res_col]) else 0
    churned_rev = row[churned_col] if hasattr(row, churned_col) and pd.notnull(row[churned_col]) else 0
    expansion_rev = row[exp_col] if hasattr(row, exp_col) and pd.notnull(row[exp_col]) else 0
    contraction_rev = row[con_col] if hasattr(row, con_col) and pd.notnull(row[con_col]) else 0
    if churned_rev + contraction_rev < 0:
        rev_qr = -1 * (new_rev + res_rev + expansion_rev) / (churned_rev + contraction_rev)
    else:
        rev_qr = math.nan
    return rev_qr



### This takes a dataframe of transactions grouped by a particular date period
### and returns active, retained, new, resurrected, and churned users for that
### time period
def calc_user_ga(x, grouping_col, first_period_col):
    au = x.loc[~x[grouping_col + '.t'].isnull(), 'user_id'].nunique() 
    ret_users = x.loc[(x['inc_amt.t'] > 0) & (x['inc_amt.l'] > 0), 'user_id'].nunique()
    new_users = x.loc[x[first_period_col + '.t'] == x[grouping_col + '.t'], 'user_id'].nunique()
    res_users = x.loc[(x[first_period_col + '.t'] != x[grouping_col + '.t']) & ~(x['inc_amt.l'] > 0), 'user_id'].nunique()
    churned_users = -1 * x.loc[~(x['inc_amt.t'] > 0), 'user_id'].nunique()
#    if churned_users < 0:
#        user_qr = -1*(new_users + res_users) / churned_users
#    else:
#        user_qr = math.nan

    vals = [au, ret_users, new_users, res_users, churned_users]
    return vals




### This takes a dataframe of transactions grouped by a particular date period
### and returns total, retained, new, expansion, resurrected, contraction, and 
### churned revenuefor that time period
def calc_rev_ga(x, grouping_col, first_period_col):
    rev = x.loc[~x[grouping_col + '.t'].isnull(), 'inc_amt.t'].sum() 
    ret_rev = x.loc[(x['inc_amt.t'] > 0) & (x['inc_amt.l'] > 0), ['inc_amt.t', 'inc_amt.l']].min(axis=1).sum()
    new_rev = x.loc[x[first_period_col + '.t'] == x[grouping_col + '.t'], 'inc_amt.t'].sum()
    res_rev = x.loc[(x[first_period_col + '.t'] != x[grouping_col + '.t']) & ~(x['inc_amt.l'] > 0), 'inc_amt.t'].sum()
    churned_rev = -1 * x.loc[~(x['inc_amt.t'] > 0), 'inc_amt.l'].sum()
    exp_rev_set = x.loc[(x[first_period_col + '.t'] != x[grouping_col + '.t']) & 
                        (x['inc_amt.t'] > 0) & (x['inc_amt.l'] > 0) &
                        (x['inc_amt.t'] > x['inc_amt.l']), ['inc_amt.t', 'inc_amt.l']]
    exp_rev = exp_rev_set['inc_amt.t'].sum() - exp_rev_set['inc_amt.l'].sum()

    con_rev_set = x.loc[(x[first_period_col + '.t'] != x[grouping_col + '.t']) & 
                        (x['inc_amt.t'] > 0) & (x['inc_amt.l'] > 0) &
                        (x['inc_amt.t'] < x['inc_amt.l']), ['inc_amt.t', 'inc_amt.l']]
    con_rev = con_rev_set['inc_amt.t'].sum() - con_rev_set['inc_amt.l'].sum()
    
#    if churned_rev + con_rev < 0:
#        rev_qr = -1*(new_rev + res_rev + exp_rev) / (churned_rev + con_rev)
#    else:
#        rev_qr = math.nan
    
    vals = [rev, ret_rev, new_rev, res_rev, exp_rev, con_rev, churned_rev]
    return vals




### Produces the "final" growth accounting dataframe with both user and
### revenue numbers for each time period in the "decorated" dataframe
def create_growth_accounting_dfs(xau_decorated_df, time_period, keep_last_period = True):
    print('Creating Growth Accounting dataframes')
    time_fields = get_time_period_dict(time_period)
    grouping_col = time_fields['grouping_col']
    first_period_col = time_fields['first_period_col']
    frequency = time_fields['frequency']
    
    xau_decorated_df_last = xau_decorated_df.copy()
    xau_decorated_df_last[grouping_col + '_join'] = xau_decorated_df_last['Next_' + grouping_col]
    xau_decorated_df[grouping_col + '_join'] = xau_decorated_df[grouping_col]
    
    xga_interim = pd.merge(xau_decorated_df, xau_decorated_df_last, 
                           suffixes = ['.t', '.l'],
                           how = 'outer', left_on = ['user_id', grouping_col + '_join'], 
                           right_on = ['user_id', grouping_col + '_join'])
                           
    user_xga = xga_interim.groupby(grouping_col + '_join')\
                .apply(lambda x: pd.Series(calc_user_ga(x, grouping_col, first_period_col),
                                           index = [frequency + ' Active Users', 
                                                    'Retained Users', 
                                                    'New Users', 
                                                    'Resurrected Users',
                                                    'Churned Users'
                                                    ]))\
                .reset_index()\
                .rename(columns = {grouping_col + '_join' : grouping_col})
                
    rev_xga = xga_interim.groupby(grouping_col + '_join')\
                .apply(lambda x: pd.Series(calc_rev_ga(x, grouping_col, first_period_col),
                                           index = [frequency + ' Revenue',
                                                    'Retained Revenue',
                                                    'New Revenue',
                                                    'Resurrected Revenue',
                                                    'Expansion Revenue',
                                                    'Contraction Revenue',
                                                    'Churned Revenue'
                                                    ]))\
                .reset_index()\
                .rename(columns = {grouping_col + '_join' : grouping_col})
                
    user_xga = user_xga[user_xga[frequency + ' Active Users'] > 0]
    rev_xga = rev_xga[rev_xga[frequency + ' Revenue'] > 0]
    
    user_xga[grouping_col] = pd.to_datetime(pd.PeriodIndex(user_xga[grouping_col]).start_time) + timedelta(hours = 7) 
    rev_xga[grouping_col] = pd.to_datetime(pd.PeriodIndex(rev_xga[grouping_col]).start_time) + timedelta(hours = 7)
    
    if not keep_last_period:
        user_xga = user_xga[:-1]
        rev_xga = rev_xga[:-1]

#    user_xga['BOM Users'] = user_xga[frequency + ' Active Users'].shift(1)
#    rev_xga['BOM Revenue'] = rev_xga[frequency + ' Revenue'].shift(1)
    
#    user_xga[frequency + ' Retention Rate'] = user_xga['Retained Users'] / user_xga['BOM Users']
#    rev_xga[frequency + ' Retention Rate'] = rev_xga['Retained Revenue'] / rev_xga['BOM Revenue']

    return user_xga, rev_xga




### Using the numbers in the "final" growth accounting dataframe, calculate
### the number of users at the beginning of the period (BOP), the  
### period-over-period user retention ratio, and the user quick ratio
def calc_user_ga_ratios(user_xga_df, time_period):
    
    time_fields = get_time_period_dict(time_period)
    frequency = time_fields['frequency']
    per = time_fields['period_abbr']
    
    ratio_df = user_xga_df.copy()
    ratio_df['Users BOP'] = ratio_df[frequency + ' Active Users'].shift(1)
    ratio_df[per + 'o' + per + ' User Retention'] = ratio_df['Retained Users'] / ratio_df['Users BOP']
    ratio_df['User Quick Ratio'] = ratio_df.apply(lambda x: calc_user_qr(x, 
            new_col = 'New Users', res_col = 'Resurrected Users', 
            churned_col = 'Churned Users'), axis = 1)
    return ratio_df




### Using the numbers in the "final" growth accounting dataframe, calculate
### the revenue at the beginning of the period (BOP), the  
### period-over-period revenue retention ratio, and the revenue quick ratio
def calc_rev_ga_ratios(rev_xga_df, time_period):
    
    time_fields = get_time_period_dict(time_period)
    frequency = time_fields['frequency']
    per = time_fields['period_abbr']
    
    ratio_df = rev_xga_df.copy()
    ratio_df['Revenue BOP'] = ratio_df[frequency + ' Revenue'].shift(1)
    ratio_df[per + 'o' + per + ' Revenue Retention'] = ratio_df['Retained Revenue'] / ratio_df['Revenue BOP']
    ratio_df['Revenue Quick Ratio'] = ratio_df.apply(lambda x: calc_rev_qr(x,
            new_col = 'New Revenue', res_col = 'Resurrected Revenue', 
            exp_col = 'Expansion Revenue', churned_col = 'Churned Revenue', 
            con_col = 'Contraction Revenue'), axis = 1)
    return ratio_df




### Join the user growth accounting dataframe with the revenue growth accounting
### dataframe
def consolidate_ga_dfs(user_ga_df, rev_ga_df, time_period):
    
    time_fields = get_time_period_dict(time_period)
    grouping_col = time_fields['grouping_col']
    
    consolidated_ga_df = pd.merge(user_ga_df, rev_ga_df,
                                  how = 'inner', on = grouping_col)

    return consolidated_ga_df




### Bring together all the Weekly/Monthly Growth Accounting Functions into a complete
def consolidate_all_ga(xau_decorated_df, time_period, keep_last_period = True):
    user_ga, rev_ga = create_growth_accounting_dfs(xau_decorated_df, time_period, keep_last_period)
    user_ga_with_ratios = calc_user_ga_ratios(user_ga, time_period)
    rev_ga_with_ratios = calc_rev_ga_ratios(rev_ga, time_period)
    all_ga_df = consolidate_ga_dfs(user_ga_with_ratios, rev_ga_with_ratios, time_period)
    return all_ga_df




### Calculate the user retention by cohort defined by any weekly or monthly time period
def xau_retention_by_cohort_df(xau_decorated_df, time_period, recent_periods_back_to_exclude = 1):
    
    time_fields = get_time_period_dict(time_period)
    grouping_col = time_fields['grouping_col']
    first_period_col = time_fields['first_period_col']
    unit = time_fields['unit']
    period_abbr = time_fields['period_abbr']
        
    since_col = '%ss Since First' % unit
    
    xau_d = xau_decorated_df.copy()
    xau_d[since_col] = xau_d[grouping_col] - xau_d[first_period_col]
    
    xau_d = xau_d.groupby([first_period_col, grouping_col, since_col])\
                    .agg({'inc_amt' : 'sum', 
                          'user_id' : 'nunique'})\
                    .rename(columns = { 'user_id' : 'cust_ct' })
    xau_d['cohort_cust_ct'] = xau_d.groupby([first_period_col])['cust_ct'].transform('first')
    xau_d['cum_inc_amt'] = xau_d.groupby([first_period_col])['inc_amt'].cumsum()
    
    xau_d['cum_inc_per_cohort_cust'] = xau_d['cum_inc_amt'] / xau_d['cohort_cust_ct']
    xau_d['cust_ret_pct'] = xau_d['cust_ct'] / xau_d['cohort_cust_ct']
    
    xau_d = xau_d.reset_index()
    
    if time_period == 'month':
        td = timedelta(months = recent_periods_back_to_exclude)
    elif time_period == 'week':
        td = timedelta(weeks = recent_periods_back_to_exclude)
    
#    curr_per = pd.to_datetime(datetime.today()).to_period(period_abbr)
    last_period = pd.to_datetime(datetime.today() - td).to_period(period_abbr)
    
    xau_d = xau_d.loc[xau_d[grouping_col] <= last_period]
    
#    if incl_curr_per:
#        xau_d = xau_d.loc[xau_d[grouping_col] <= curr_per]
#    else:
#        print(grouping_col, curr_per)
#        print(xau_d.head())
#        xau_d = xau_d.loc[xau_d[grouping_col] < curr_per]
    
    return xau_d




### For a particular "last_date" of a time period window, assigns a designation
### to each DAU in the DAU dataframe. The options are "this_period," which is in
### the last window_days days; "first_this_period," which indicates that the user 
### new in the window in question; and "last_period," which means it was between
### 1X and 2X windows_days in the past
def assign_ga_date_range(row, last_date, window_days):
    ga_date_range = 'this_period'
    curr_period_start_dt = last_date - timedelta(days = window_days-1)
    prev_period_start_dt = last_date - timedelta(days = 2*window_days-1)
    
    if row['first_dt'] >= curr_period_start_dt:
        ga_date_range = 'first_this_period'
    elif row['activity_date'] >= prev_period_start_dt and row['activity_date'] < curr_period_start_dt:
        ga_date_range = 'last_period'
    
    return ga_date_range



### After grouping by the date ranges in assign_ga_date_range, this determines
### how the aggregate count and sum should be classified according to growth
### accounting definitions
def assign_user_status(x):
    is_last_period = pd.notnull(x.last_period)
    is_this_period = pd.notnull(x.this_period)
    is_first_this_period = pd.notnull(x.first_this_period)
    
    if is_first_this_period:
        status = 'new'
    elif is_last_period and is_this_period:
        status = 'retained'
    elif is_this_period and not is_last_period:
        status = 'resurrected'
    elif is_last_period and not is_this_period:
        status = 'churned'
    else:
        status = 'prior'

    return status



### Combines the above functions to determine the growth accounting for a 
### window specified by its end date. If use_segment is False, it returns a
### dataframe of one row. If use_segment is True, it returns a dataframe with 
### one row per segment
def calc_ga_for_window(dau_decorated_df, last_date, window_days, use_segment):
    window_start_date = last_date - timedelta(days = 2*window_days-1)
    dau_dec = (dau_decorated_df
            .loc[(dau_decorated_df['activity_date'] >= window_start_date) & (dau_decorated_df['activity_date'] <= last_date)]
            .copy()
            )
    
    dau_dec['ga_date_range'] = dau_dec.apply(lambda x: assign_ga_date_range(x, last_date, window_days), axis = 1)
    
    groupings = ['user_id', 'ga_date_range']
    if use_segment:
        groupings.insert(1, 'segment')
        
    dau_grouped = (dau_dec.groupby(groupings)['inc_amt']
                                .sum()
                                .unstack()
                                .reset_index()
                                )
          
    dau_grouped['user_status'] = dau_grouped.apply(assign_user_status, axis = 1)

    groupings2 = ['user_status']
    if use_segment == True:
        groupings2.insert(0, 'segment')
        
        dau_grouped_2 = (dau_grouped.groupby(groupings2)['user_id']
                            .count()
                            .unstack(level = 'user_status')
                            .reset_index()
                            )
        dau_grouped_2['window_end_date'] = last_date
    else:
        dau_grouped_2 = (dau_grouped.groupby(groupings2)['user_id']
                            .count()
                            .to_frame(name = last_date)
                            .T
                            .reset_index()
                            .rename(columns = { 'index' : 'window_end_date'})
                            )  
    
    new_users = dau_grouped_2.new if hasattr(dau_grouped_2, 'new') else 0 
    res_users = dau_grouped_2.resurrected if hasattr(dau_grouped_2, 'resurrected') else 0
    churned_users = dau_grouped_2.churned if hasattr(dau_grouped_2, 'churned') else 0
    ret_users = dau_grouped_2.retained if hasattr(dau_grouped_2, 'retained') else 0

    dau_grouped_2['user_quick_ratio'] = dau_grouped_2.apply(calc_user_qr, axis = 1)
    dau_grouped_2['user_retention_rate'] = ret_users / (ret_users + churned_users)
    dau_grouped_2['window_days'] = window_days
        
    return dau_grouped_2
    

    
### Calls calc_ga_for_window for each available window of length window_days
### and compiles the resultant dataframe into a single dataframe for plotting
### and analysis
def calc_rolling_qr_window(dau_decorated_df, window_days = 28, use_segment = False):
    start_dt = min(dau_decorated_df['activity_date']) + timedelta(days = 2*window_days)
    end_dt = max(dau_decorated_df['activity_date'])
    date_range = pd.date_range(start = start_dt, end = end_dt, freq = 'D')

    rolling_qr_df = pd.DataFrame()
    for d in date_range:
        d2 = d.date()
        print(window_days, d2)
        this_window = calc_ga_for_window(dau_decorated_df, d2, window_days, use_segment)
        rolling_qr_df = rolling_qr_df.append(this_window)
    return rolling_qr_df

################### NEW AS OF 11/5/18

    

def calc_user_daily_usage(dau_decorated_df, last_date, window_days, breakouts):
    window_start_date = last_date - timedelta(days = window_days-1)
    dau_dec = (dau_decorated_df
               .loc[(dau_decorated_df['activity_date'] >= window_start_date) & 
                    (dau_decorated_df['activity_date'] <= last_date)]
               .copy()
               )
    dau_grouped_df = (dau_dec
                      .groupby(['user_id'])['inc_amt']
                      .agg(['count', 'sum'])
                      .reset_index()
                      .rename(columns = {'count' : 'active_days', 'sum': 'inc_amt' })
                      )
    for b in breakouts:
        dau_grouped_df['%sd+ users' % b] = (dau_grouped_df['active_days'] >= b)
        
    return dau_grouped_df
        



def calc_dau_xau_ratio_for_window(dau_decorated_df, last_date, window_days, breakouts):
    dau_grouped = calc_user_daily_usage(dau_decorated_df, last_date, window_days, breakouts)
    
    dau_agg = pd.DataFrame()
    dau_agg['active_days'] = pd.Series(dau_grouped.active_days.sum())
    dau_agg['1d+ users'] = dau_grouped.user_id.nunique()
    dau_agg['dau_window_ratio'] = (dau_agg['active_days'] / window_days) / dau_agg['1d+ users']
    dau_agg['window_frequency'] = dau_agg['dau_window_ratio'] * window_days
    for b in breakouts:
        col_name = '%sd+ users' % b
        dau_agg[col_name] = dau_grouped[col_name].sum()
        ratio_col_name = '%sd+ users per xau' % b
        dau_agg[ratio_col_name] = dau_agg[col_name] / dau_agg['1d+ users']
    dau_agg['window_end_dt'] = last_date
        
    return dau_agg




def create_dau_window_df(dau_decorated_df, window_days = 28, breakouts = [2, 4]):
    start_dt = min(dau_decorated_df['activity_date']) + timedelta(days = window_days)
    end_dt = max(dau_decorated_df['activity_date'])
    date_range = pd.date_range(start = start_dt, end = end_dt, freq = 'D')

    rolling_dau_xau_df = pd.DataFrame()
    for d in date_range:
        d2 = d.date()
        print(window_days, d2)
        this_window = calc_dau_xau_ratio_for_window(dau_decorated_df, 
                                                    last_date = d2, 
                                                    window_days = window_days, 
                                                    breakouts = breakouts)
        rolling_dau_xau_df = rolling_dau_xau_df.append(this_window)
    return rolling_dau_xau_df

    
    
    
    
    