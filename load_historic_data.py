import pandas as pd
import requests
import sqlite3
import time

# Function to fetch data
def fetch_precip_data(station_id, start_date, end_date):
    url = "https://www.ncei.noaa.gov/access/services/data/v1"
    params = {
        "dataset": "daily-summaries",
        "stations": station_id,
        "dataTypes": "PRCP",
        "startDate": start_date,
        "endDate": end_date,
        "format": "json",
        "units": "standard"
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Failed for {station_id}: {e}")
        return 'Exception'

# Connect to DB and pull all stations
db = "data/precip_data_ca.db"
conn = sqlite3.connect(db)
sql = 'SELECT * FROM stations'
stations_df = pd.read_sql(sql, conn)

# Fetch and save data to DB. Define time period for data pull.
start_date = "2023-07-22"
end_date = "2025-07-22"

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

# Check how many rows inserted
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM precip")
print(f"Total rows inserted: {cur.fetchone()[0]}")
conn.close()