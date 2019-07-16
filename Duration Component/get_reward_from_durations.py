import pandas as pd
from duration_reward import duration_reward

# Read the raw data file into a dataframe
data_file = './data/device_active_times.txt'

duration_data_frame = pd.read_csv(data_file, sep = '|')

active_times = duration_data_frame['active_time'].values
active_time_rewards = duration_reward(active_times)

duration_data_frame['reward'] = active_time_rewards

duration_data_frame.to_csv('./data/device_active_time_rewards.txt', sep = '|', index = False)