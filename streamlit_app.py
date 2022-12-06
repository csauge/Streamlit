import streamlit as st
import pandas as pd
import requests

st.write("# Liste des Vélib'")

url = 'https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=1500'
data = requests.get(url).json()
df = pd.json_normalize(data, "records")
df[['lat','lon']] = pd.DataFrame(df["fields.coordonnees_geo"].tolist(), index=df.index)
df
st.map(df)
data
