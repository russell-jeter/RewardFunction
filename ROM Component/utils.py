import numpy as np

def normalize_rom(rom_list):
    
    if len(rom_list) != 2:
        raise ValueError('You are not passing the right list.  It should be of the form [rom_min, rom_max].')
    
    rom_min, rom_max = rom_list

    if rom_min > rom_max:
        temp = rom_max
        rom_max = rom_min
        rom_min = temp
    
    normalized_min = (rom_min + 40.0)/(60 + 40)
    normalized_max = (rom_max + 40.0)/(60 + 40)

    return [normalized_min, normalized_max]