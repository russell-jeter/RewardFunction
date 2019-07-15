import psycopg2
import pandas as pd
import datetime

from config import *

# Get the configuration parameters for the database connection.
params = config()

# Get yesterday's date to ensure we're collecting the right data.
today     = datetime.datetime.today().date()
yesterday = today - datetime.timedelta(days = 1)
one_week  = yesterday - datetime.timedelta(days = 7)

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
            SELECT "deviceId", "sessionId", "clientDate", "activity", "state", "text"
            FROM status_patientdata
            WHERE "deviceId" = '{}'
            AND "clientDate" > '{} 00:00:00'::timestamp
            AND "clientDate" < '{} 23:59:59'::timestamp;
            """.format(deployed_devices, one_week, yesterday)

else:

    deployed_devices = str(deployed_devices)

    query = """
            SELECT "deviceId", "sessionId", "clientDate", "activity", "state", "text"
            FROM status_patientdata
            WHERE "deviceId" in {}
            AND "clientDate" > '{} 00:00:00'::timestamp
            AND "clientDate" < '{} 23:59:59'::timestamp;
            """.format(deployed_devices, one_week, yesterday)

cur.execute(query)

# Fetch table output from the query.
colnames = [desc[0] for desc in cur.description]
rows     = cur.fetchall()

# Convert table returned from the SQL query to a pandas dataframe for saving.
rows_df = pd.DataFrame(rows, columns = colnames)

print('Saving the file.....')
data_file = './data/user_use_data.txt'
rows_df.to_csv(data_file, sep = '|', index = False)

if conn is not None:
    conn.close()
    print('Database connection closed.')
