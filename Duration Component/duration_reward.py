import numpy as np

def duration_reward(durations):
    
    rewards = ((1.0 + np.tanh(0.045 * (durations - 60.0))) / 2.0)
    
    rewards[durations == 0] = 0
    
    return rewards
