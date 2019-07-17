import pandas as pd
import numpy as np
import datetime
import pytz
import json

'''
This script converts all of the raw data from the database into session level activity durations.
'''
# Read the raw data file into a dataframe
data_file = './data/user_use_data.txt'

data_frame = pd.read_csv(data_file, sep = '|')

data_frame['clientDate'] = pd.to_datetime(data_frame['clientDate']).dt.round('1s')
# Set of states that we want to keep.  These are the activities that inform the user activity status. 
states_to_keep = [ 
 'CalComplete', 'InitialMenuActivity.onResume()',
 'InitialMenuActivity.startHelpAction()',
 'InitialMenuActivity.startTherapyAction()', 'CalStart',
 'InitialMenuActivity.swapMenuSettings()',
 'InitialMenuActivity.swapMenuAudioSettings()',
 'InitialMenuActivity.swapMenuMain()', 
 'Games.initAndStartGame()', 'PanicPressed',
 'PeripheralDisconnected', 'UnityReceiver.onReceive():LogJSON',
 'UnityReceiver.onReceive():GameStart',
 'InitialMenuActivity.startMyReportsAction()'
]

# There is an issue with the server timestamp mapping to a time in 2013.  We create a flag for 
# rows that have this issue, so that we can decide later if we want to delete them.

data_frame = data_frame[data_frame['state'].isin(states_to_keep)]
data_frame = data_frame.sort_values(by = ['deviceId', 'clientDate'])

session_device_df = data_frame[['sessionId', 'deviceId']]
session_device_df = session_device_df.drop_duplicates()

session_list          = np.array(session_device_df['sessionId'].tolist())
device_list           = np.array(session_device_df['deviceId'].tolist())

# For each session, compute it's total length by subtracting the first time point in that session by the last time.

session_duration_list = []
start_time_list       = []
timestamp_error_list  = []

N = len(session_list)
for i in range(N):
    
    session    = session_list[i]
    session_df = data_frame[data_frame['sessionId'].isin([session])]
    start_time = session_df['clientDate'].iloc[0]
    end_time   = session_df['clientDate'].iloc[-1]
    
    duration      = end_time - start_time
    duration_in_s = duration.total_seconds()
    duration_in_m = divmod(duration_in_s, 60)[0] 
    
    session_duration_list.append(duration_in_m)
    start_time_list.append(start_time)

session_duration_list = np.array(session_duration_list)

# Remove sessions that last less than a minute
data_frame = data_frame[~ data_frame['sessionId'].isin(session_list[session_duration_list < 1.0])]

data_frame = data_frame.sort_values(by = ['deviceId', 'sessionId', 'clientDate'])

data_frame['text'] = data_frame['text'].str.lower().str.replace('""', '"')

indices           = data_frame[data_frame['state'].isin(['UnityReceiver.onReceive():LogJSON'])].index
temp_frame        = data_frame.loc[indices, ['text']]
indices_to_remove = temp_frame[~temp_frame['text'].str.contains('"gamestate":')].index

new_frame = data_frame.drop(indices_to_remove)

new_frame = new_frame.sort_values(by = ['deviceId', 'sessionId', 'clientDate'])
new_frame = new_frame.drop_duplicates()
# Replace problematic text patterns from the text dump field.
new_frame['text'] = new_frame['text'].str.replace('""', '"')
new_frame['text'] = new_frame['text'].str.replace('\\', '')
new_frame['text'] = new_frame['text'].str.replace('"{', '{')
new_frame['text'] = new_frame['text'].str.replace('}"', '}')
new_frame['text'] = new_frame['text'].str.replace('"scoredata":",' , '')
new_frame['text'] = new_frame['text'].str.replace(',"gamestate":"}', '}')


new_frame['active_passive'] = 'None'
# Compute the indices of the dataframe that correspond to unity log dumps.
indices        = new_frame[new_frame['state'].isin(['UnityReceiver.onReceive():LogJSON'])].index
error_list     = []
gamestate_list = []

# Text fields that correspond to beginning an active user phase.
active_list = ['gamestart', 'resumegame', 'gameplay',
       'start', 'startgame']
# Text fields that correspond to ending an active user phase.
passive_list = ['endgame', 'gameover', 'gamepause',
       'gamequit',  'resetgame', 'stopgame']

# Populate active and passive states for events
for index in indices:
    json_string = new_frame.loc[index,'text']
    json_data   = json.loads(json_string)
    
    if 'gamestate' in json_data.keys():
        gamestate_list.append(json_data['gamestate'])
    
        if json_data['gamestate'] in active_list:
            new_frame.loc[index, 'active_passive'] = 'Active'
    
        if json_data['gamestate'] in passive_list:
            new_frame.loc[index, 'active_passive'] = 'Passive'

new_frame.loc[new_frame['state'].isin(['CalStart']), 'active_passive']                            = 'Active'
new_frame.loc[new_frame['state'].isin(['CalComplete']), 'active_passive']                         = 'Passive'
new_frame.loc[new_frame['state'].isin(['UnityReceiver.onReceive():GameStart']), 'active_passive'] = 'Active'
new_frame.loc[new_frame['text'].isin(['panic pressed']), 'active_passive']                        = 'Passive'

activity_frame = new_frame
activity_frame = activity_frame.sort_values(by = ['deviceId', 'sessionId', 'clientDate'])

active_duration_list = []

N = len(session_list)
# Using the activity state fields, compute the total active time for a session.
for i in range(N):
    active_duration = 0    
    
    session       = session_list[i]
    session_df    = activity_frame[activity_frame['sessionId'].isin([session])]
    start_time    = False
    previous_time = 'None'
    for index, row in session_df.iterrows():
        
        if row['active_passive'] == 'Active':
        
            if previous_time == 'None' or previous_time == 'Stop':
        
                start_time    = row['clientDate']
                previous_time = 'Start'
        
            elif previous_time == 'Start':
        
                end_time      = row['clientDate']
                duration      = end_time - start_time
                duration_in_s = duration.total_seconds()
                duration_in_m = divmod(duration_in_s, 60)[0]
                duration      = duration_in_m
        
                if duration < 360:
                    active_duration = active_duration + duration
        
                start_time = end_time
            
            previous_time = 'Start'
        
        elif row['active_passive'] == 'Passive':
        
            if start_time:
        
                end_time = row['clientDate']
                duration = end_time - start_time
                duration_in_s = duration.total_seconds()
                duration_in_m = divmod(duration_in_s, 60)[0]
                duration = duration_in_m
        
                if duration < 360:
                    active_duration = active_duration + duration
        
                start_time = False
        
            previous_time = 'Stop'
    
    active_duration_list.append(active_duration)

active_duration_list = np.array(active_duration_list)

# Compute passive time from total time and active time.

passive_duration_list = session_duration_list - active_duration_list

column_list = ['session_id', 'device_id', 'total_duration', 'active_duration', 
               'passive_duration', 'start_time']

session_duration_device_user_list = []

N = len(session_list)

# Put all of the elements of the table into a list.
for i in range(N):

    session          = session_list[i]
    duration         = session_duration_list[i]
    active_duration  = active_duration_list[i]
    passive_duration = passive_duration_list[i]
    start_time       = start_time_list[i].date()
    device           = device_list[i]
    
    session_duration_device_user_list.append([session, device, duration, active_duration, 
                                              passive_duration, start_time])

session_duration_device_user_table = pd.DataFrame(session_duration_device_user_list, columns = column_list)

device_list = np.unique(device_list)
consistency_list = []

active_date_list = []

for device in device_list:
    
    days_active = 0.
    dates = session_duration_device_user_table['start_time'].unique()

    for date in dates:

        date_active_sum = session_duration_device_user_table[session_duration_device_user_table['start_time'] == date]['active_duration'].sum()
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