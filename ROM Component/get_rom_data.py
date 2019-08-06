"""Tutorial for using pandas and the InfluxDB client."""

import pandas as pd
import datetime

from influxdb import DataFrameClient
from influx_config import config

params = config()

host     = params['host']
port     = params['port']
database = params['database']
user     = params['user']
password = params['password']

# Store date in January 2016.  Any data before then is not actually valid.
cutoff_date     = datetime.date(2016, 1, 1)

# Load the list of deployed devices.
user_file        = open('../deployed_devices.txt',"r") 
deployed_devices = user_file.readlines()
user_file.close() 

# Strip the carriage returns and new lines from the strings and convert the list to a tuple
deployed_devices = [line.rstrip("\r\n") for line in deployed_devices]
deployed_devices = tuple(deployed_devices)


where_device_string = ''

for i in range(len(deployed_devices)):
    if i == 0:

        where_device_string = "device_id = '{}'".format(deployed_devices[i])

    else:

        where_device_string = where_device_string + " or device_id = '{}'".format(deployed_devices[i])


def main():

    
    """Instantiate the connection to the InfluxDB client."""

    client = DataFrameClient(host, port, user, password, database)

    query = """SELECT * from device 
    WHERE {} 
    """.format(where_device_string)
    
    result = client.query(query)

    data_frame = result['device']

    data_file = './data/rom_data.txt'
    data_frame.to_csv(data_file, sep = '|', index = True, index_label = 'time')


if __name__ == '__main__':
    main()
