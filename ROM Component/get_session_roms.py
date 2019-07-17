import pandas as pd
import numpy as np
import datetime
import pytz
from utils import normalize_rom

'''
This script converts the raw angle data from the database into session level min ROMs and max ROMs.
'''
# Read the raw data file into a dataframe
data_file  = './data/rom_data.txt'
data_frame = pd.read_csv(data_file, sep = '|')

data_frame['clientDate'] = pd.to_datetime(data_frame['clientDate']).dt.round('1s')

# Clip the timestamps and remove duplicates.
# This helps us weed out the time points that are from the inactive periods for the device. 
data_frame = data_frame.drop_duplicates()
data_frame = data_frame.sort_values(by = ['clientDate'])

# Get a list of unique sessions
sessions = data_frame['sessionId'].unique()

rom_min_list        = []
rom_max_list        = []
session_list        = []
start_time_list     = []
session_date_list   = []
session_device_list = [] 

suspect_sessions = []

# Get the min and max ROM for each session.
for session in sessions:

    session_angles = data_frame[data_frame['sessionId'] == session]['angle'].values
    start_time     = data_frame[data_frame['sessionId'] == session]['clientDate'].min()

    # Compute the upper and lower 0.5 percentile so that we can do a basic filter of instantaneous garbage data
    upper_percentile = np.percentile(session_angles, 99.5)
    lower_percentile = np.percentile(session_angles, 0.5)
    
    # If we do have a ridiculously high (or low) upper (lower) 0.5 percentile, we'll record the 
    # session ID, so we can inspect it by eye.
    if upper_percentile >= 70:
        suspect_sessions.append([session, upper_percentile])
    
    if lower_percentile <= -50:
        suspect_sessions.append([session, lower_percentile])

    # Replace the outliers with the mean.
    session_mean = np.mean(session_angles)
    session_angles[session_angles >= upper_percentile * 0.99] = session_mean
    session_angles[session_angles <= lower_percentile * 1.01] = session_mean

    rom_min = np.min(session_angles)
    rom_max = np.max(session_angles)
    
    # If the min ROM and max ROM for a session are the same, it's garbage data, so we'll ignore
    # this session.

    if not rom_min == rom_max:

        session_date   = data_frame[data_frame['sessionId'] == session]['clientDate'].dt.date.tolist()[0]
        session_device = data_frame[data_frame['sessionId'] == session]['deviceId'].tolist()[0]
        rom_min_list.append(rom_min)
        rom_max_list.append(rom_max)
        session_list.append(session)
        session_date_list.append(session_date)
        session_device_list.append(session_device)

# Push the min ROM and max ROM data to a data frame so that we can save it neatly.
column_list = ['device_id', 'session_id', 'session_date', 'rom_min', 'rom_max']

session_rom_list = []

N = len(session_list)

# Put all of the elements of the table into a list.
for i in range(N):

    session  = session_list[i]
    rom_min  = rom_min_list[i]
    rom_max  = rom_max_list[i]
    device   = session_device_list[i]
    date     = session_date_list[i]
    
    session_rom_list.append([device, session, date, rom_min, rom_max])

session_rom_table = pd.DataFrame(session_rom_list, columns = column_list)

session_rom_table.to_csv('./data/session_roms.txt', sep = '|', index = False)

column_list = ['session_id', 'upper/lower percentile']

suspect_sessions_table = pd.DataFrame(suspect_sessions, columns = column_list)

suspect_sessions_table.to_csv('./data/suspect_rom_sessions.txt', sep = '|', index = False)