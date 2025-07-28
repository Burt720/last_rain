import pandas as pd
import geopandas as gpd
import sqlite3
from shapely.geometry import Point
import requests
import csv
import os

# Get list of 50 states + DC. Optional if only pulling specific state
csv_path = "data/states.csv"

states = []

with open(csv_path, 'r', newline='') as file:
    csv_reader = csv.reader(file)
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        states.append(row[0])

# Download station list
stations_url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
stations_file = "data/ghcnd-stations.txt"

if not os.path.exists(stations_file):
    print("Downloading station list...")
    response = requests.get(stations_url)
    with open(stations_file, "wb") as f:
        f.write(response.content)

# Parse stations file
colspecs = [(0,11),(12,20),(21,30),(31,37),(38,40),(41,71)]
names = ['ID', 'LAT', 'LON', 'ELEV', 'STATE', 'NAME']
stations_df = pd.read_fwf(stations_file, colspecs=colspecs, names=names)
#stations_df = stations_df[stations_df['STATE'].isin(states)] # Uncomment to use list of all states to pull entire US
stations_df = stations_df[stations_df['STATE'] == 'CA'] # Filter for just 1 state

# Create DB
db = "data/precip_data_ca.db" # 1 state DB
conn = sqlite3.connect(db) 

# Add station list to table
stations_df.to_sql("stations", conn, if_exists="replace", index=False)

# Open ZCTA (zip code) file - download and extract from https://www2.census.gov/geo/tiger/TIGER2022/ZCTA520/tl_2022_us_zcta520.zip
file = "data/zcta/tl_2022_us_zcta520.shp"
zcta_gdf = gpd.read_file(file)
zcta_gdf = zcta_gdf.to_crs("EPSG:4326")

# Spatial join stations to zip codes
sql = 'SELECT * FROM stations'
stations_df = pd.read_sql(sql, conn)
stations_df['geometry'] = stations_df.apply(lambda row: Point(row['LON'], row['LAT']), axis=1)
stations_gdf = gpd.GeoDataFrame(stations_df, geometry='geometry', crs='EPSG:4326')
stations_gdf = gpd.sjoin(stations_gdf, zcta_gdf, how='left')
stations_gdf = stations_gdf.drop(columns=['geometry', 'index_right', 'GEOID20', 'CLASSFP20', 'MTFCC20', 'FUNCSTAT20', 'ALAND20', 'AWATER20', 'INTPTLAT20', 'INTPTLON20'])
stations_gdf = stations_gdf.rename(columns={'ZCTA5CE20': 'ZIPCODE'})

# Replace stations table and close DB connection
stations_gdf.to_sql("stations", conn, if_exists="replace", index=False)
conn.close()