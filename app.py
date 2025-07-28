import streamlit as st
import pandas as pd
import sqlite3
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime
import altair as alt

# Connect to DB
db = "data/precip_data_ca.db"
conn = sqlite3.connect(db)

# Query DB and convert results to GeoDataFrame
sql = '''
        SELECT 
            ID,
            NAME,
            ZIPCODE,
            STATE,
            LAT,
            LON,
            MAX(DATE) AS LAST_RAIN
        FROM
            precip, stations
        WHERE
            precip.STATION = stations.ID AND 
            PRCP IS NOT NULL AND
            PRCP > 0.001 
        GROUP BY 1'''
df = pd.read_sql(sql, conn)
df['LAST_RAIN'] = pd.to_datetime(df['LAST_RAIN']).dt.date
df['DAYS_SINCE_RAIN'] = (datetime.today().date() - df['LAST_RAIN']).apply(lambda x: x.days)
df['geometry'] = df.apply(lambda row: Point(row['LON'], row['LAT']), axis=1)
gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")

# Get date of latest data
sql = '''SELECT MAX(DATE) FROM precip'''
cur = conn.cursor()
cur.execute(sql)
last_date = cur.fetchone()[0]
conn.close()

# Title
st.title("When Did It Rain?")
st.markdown("Search by place name, ZIP code, or station ID to see how many days it's been since it last rained.")
st.markdown(f"NOAA station observations can be delayed up to 5 days. Results below reflective of data up to {last_date}.")

# Search input
search_input = st.text_input("Enter place, ZIP code, or station ID:").strip().upper()

# Filter logic
if search_input:
    results = gdf[gdf['ID'].str.upper().str.contains(search_input) |
                  gdf['NAME'].str.upper().str.contains(search_input) |
                  gdf['ZIPCODE'].str.upper().str.contains(search_input)]
    if not results.empty:
        st.success(f"Found {len(results)} matching station(s)")
        st.dataframe(results[['ID', 'NAME', 'ZIPCODE', 'STATE', 'LAST_RAIN', 'DAYS_SINCE_RAIN']], hide_index=True)
        # Render map if streamlit-folium installed
        try:
            import streamlit_folium
            import folium
            # Create approximately 1 mile buffered bound box around results - https://www.usgs.gov/faqs/how-much-distance-does-a-degree-minute-and-second-cover-your-maps
            bounds = [[results.geometry.y.min() - 0.016, results.geometry.x.min() - 0.016],
                      [results.geometry.y.max() + 0.016, results.geometry.x.max() + 0.016]]
            
            m = folium.Map(location=[results.geometry.y.mean(), results.geometry.x.mean()])
            m.fit_bounds(bounds)
            for _, row in results.iterrows():
                folium.Marker(
                    location=[row.geometry.y, row.geometry.x],
                    popup=f"{row['NAME']}\nLast Rain: {row['LAST_RAIN']} ({row['DAYS_SINCE_RAIN']} days ago)"
                ).add_to(m)
            streamlit_folium.folium_static(m)
        except ImportError:
            st.info("Install streamlit-folium to enable map view: `pip install streamlit-folium`.")

        # Allow user to select station from results and render precipitation over time graph
        selected_station = st.selectbox("Select a station to view rainfall history:", results['NAME'], index=None)
        if selected_station:
            conn = sqlite3.connect(db)
            sql = f'''
                    SELECT 
                        DATE, 
                        PRCP
                    FROM 
                        precip
                    WHERE 
                        STATION = (SELECT ID FROM stations where NAME ='{selected_station}') AND 
                        PRCP IS NOT NULL
                    ORDER BY DATE ASC'''
            rain_df = pd.read_sql(sql, conn)
            conn.close()
  
            rain_df['DATE'] = pd.to_datetime(rain_df['DATE'])
            st.subheader(f"Rainfall Over Time for {selected_station}")
            chart = alt.Chart(rain_df).mark_bar().encode(
                x=alt.X('DATE:T', title='Date', axis=alt.Axis(format='%b %Y')),
                y=alt.Y('PRCP:Q', title='Precipitation (in)'),
                tooltip=['DATE:T', 'PRCP']
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)

    else:
        st.warning("No matches found. Try different keywords or check spelling.")
else:
    st.info("Enter a search term to begin.")