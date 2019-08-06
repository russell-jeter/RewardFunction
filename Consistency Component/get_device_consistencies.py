import pandas as pd
import numpy as np
import datetime
import pytz
import json

'''
This script converts all of the raw data from the database into session level activity durations.
'''
# Read the raw data file into a dataframe

data_file = './data/session_durations.txt'

session_duration_data_frame = pd.read_csv(data_file, sep = '|')
session_duration_data_frame['start_time'] = pd.to_datetime(session_duration_data_frame['start_time']).dt.date

device_list = np.unique(session_duration_data_frame['device_id'].values)
consistency_list = []

active_date_list = []

for device in device_list:
    
    days_active = 0.
    device_session_duration_data_frame = session_duration_data_frame[session_duration_data_frame['device_id'] == device]
    dates = device_session_duration_data_frame['start_time'].unique()

    for date in dates:

        date_active_sum = device_session_duration_data_frame[device_session_duration_data_frame['start_time'] == date]['active_duration'].sum()
        if date_active_sum >= 1.0:
            active_date_list.append([device, date, 1])
        else:
            active_date_list.append([device, date, 0])


column_list = ['device_id', 'date', 'active']
active_date_table = pd.DataFrame(active_date_list, columns = column_list)
consistency_list = []
one_week   = datetime.timedelta(days = 7)

for device in device_list:

    device_active_frame = active_date_table[active_date_table['device_id'] == device]

    for index, row in device_active_frame.iterrows():
        date = row['date']
        active = row['active']
        one_week_mask  = (date - one_week  < device_active_frame['date']) & (device_active_frame['date'] <= date)

        # Sum up active durations over the last week.
        consistency = device_active_frame.loc[one_week_mask]['active'].sum()/7.0
        consistency_list.append([device, date, consistency, active])
    
column_list = ['device_id', 'date', 'consistency', 'active']
device_consistency_table = pd.DataFrame(consistency_list, columns = column_list)

device_consistency_table.to_csv('./data/device_consistencies.txt', sep = '|', index = False)