import pandas as pd
import numpy as np
from utils import normalize_rom

'''
This script computes session-level rewards for the ROM data.
'''
# Read the session rom info file into a dataframe
data_file         = './data/session_roms.txt'
session_rom_frame = pd.read_csv(data_file, sep = '|')


session_rom_frame['rom_reward'] = 0

for index, row in session_rom_frame.iterrows():
    
    normalized_min, normalized_max = normalize_rom([row['rom_min'], row['rom_max']])

    if normalized_min < 0.4:
        min_reward = 1 - np.maximum(normalized_min, 0)
    else:
        min_reward = 0

    if normalized_max > 0.4:
        max_reward = np.minimum(normalized_max, 1)
    else:
        max_reward = 0

    '''
    # Compute reward without the clipping done using the conditionals above.
    min_reward = 1 - np.maximum(normalized_min, 0)
    max_reward = np.minimum(normalized_max, 1)
    '''
    rom_reward = np.mean([min_reward, max_reward])

    session_rom_frame.loc[index, 'rom_reward'] = rom_reward

session_rom_frame.to_csv('./data/session_roms_with_reward.txt', sep = '|', index = False)