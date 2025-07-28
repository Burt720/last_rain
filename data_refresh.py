import pandas as pd
import requests
import sqlite3
import time
from datetime import date, datetime, timedelta
from load_historic_data import fetch_precip_data

# Connect to DB and get max date stored
db = "data/precip_data_ca.db"
conn = sqlite3.connect(db)
date_sql = '''SELECT MAX(DATE) FROM precip'''
cur = conn.cursor()
cur.execute(date_sql)

# Increment max date by 1 day for start date
start_date = cur.fetchone()[0]
start_date_datetime = datetime.strptime(start_date, "%Y-%m-%d")
start_date_datetime = start_date_datetime + timedelta(days=1)
start_date = start_date_datetime.strftime("%Y-%m-%d")

# Get current date for end date
today = date.today()
end_date = today.strftime("%Y-%m-%d")

# Get all stations
stations_sql = 'SELECT * FROM stations'
stations_df = pd.read_sql(stations_sql, conn)

# Fetch and save data to DB with new start and end dates
failed_stations = []

for sid in stations_df['ID']:
    print(f"Fetching {sid}...")
    data = fetch_precip_data(sid, start_date, end_date)
    if data == 'Exception':
        failed_stations.append(sid)
    elif data:
        df = pd.DataFrame(data)
        df.to_sql("precip", conn, if_exists="append", index=False)
    time.sleep(1)

# Retry failed stations once
if failed_stations:
    for sid in failed_stations:
        print(f"Fetching {sid}...")
        data = fetch_precip_data(sid, start_date, end_date)
        if data == 'Exception':
            print('Exception')
        elif data:
            df = pd.DataFrame(data)
            df.to_sql("precip", conn, if_exists="append", index=False)
        time.sleep(1)

conn.close()