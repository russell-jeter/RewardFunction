import psycopg2
import pandas as pd
import datetime

from config import *

# Get the configuration parameters for the database connection.
params = config()

# Store date in January 2016.  Any data before then is not actually valid.
cutoff_date     = datetime.date(2016, 1, 1)

# Load the list of deployed devices.
user_file        = open('../deployed_devices.txt',"r") 
deployed_devices = user_file.readlines()
user_file.close() 

# Strip the carriage returns and new lines from the strings and convert the list to a tuple
deployed_devices = [line.rstrip("\r\n") for line in deployed_devices]
deployed_devices = tuple(deployed_devices)

# connect to the PostgreSQL server
print('Connecting to the PostgreSQL database...')
conn = psycopg2.connect(**params)
cur  = conn.cursor()

print('Performing the query......')
if len(deployed_devices) == 1:
    
    deployed_devices = deployed_devices[0]
    
    query = """
            SELECT "angle", "clientDate", "sessionId", "deviceId"
            FROM status_patientdata
            WHERE "deviceId" = '{}'
            AND "clientDate" > '{} 00:00:00'::timestamp;
            """.format(deployed_devices, cutoff_date)
else:

    deployed_devices = str(deployed_devices)

    query = """
            SELECT "angle", "clientDate", "sessionId", "deviceId"
            FROM status_patientdata
            WHERE "deviceId" in {}
            AND "clientDate" > '{} 00:00:00'::timestamp;
            """.format(deployed_devices, cutoff_date)

cur.execute(query)

# Fetch table output from the query.
colnames = [desc[0] for desc in cur.description]
rows     = cur.fetchall()

# Convert table returned from the SQL query to a pandas dataframe for saving.
rows_df = pd.DataFrame(rows, columns = colnames)

print('Saving the file.....')
data_file = './data/rom_data.txt'
rows_df.to_csv(data_file, sep = '|', index = False)

if conn is not None:
    conn.close()
    print('Database connection closed.')
