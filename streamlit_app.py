import streamlit as st
import pandas as pd
import requests

st.write("# Liste des Vélib'")

url = 'https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=1500'
data = requests.get(url).json()
df = pd.json_normalize(data, "records")
df[['lat','lon']] = pd.DataFrame(df["fields.coordonnees_geo"].tolist(), index=df.index)
#df
#st.map(df)
#data

from abc import ABC, abstractmethod
import gspread
import os
import pandas as pd
from pathlib import Path
from pyairtable.api.table import Table
import sqlalchemy as sa


class Destination():
    
    def __init__(self):
        connection_str = f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DATABASE']}"
        self.engine = sa.create_engine(connection_str)

    def load(self, sources):
        with self.engine.begin() as connection:
            for source in sources:
                df = pd.DataFrame.from_records(source.extract())
                #df.to_sql(f"_raw_{source.output_table}", connection, if_exists="replace", index=False)
                #source.transform(df).to_sql(f"{source.output_table}", connection, dtype=source.types(), if_exists="replace", index=False)
                alt.Chart(source.transform(df)).mark_bar().encode(
                    x='groupe_local',
                    y='sum(nombre_participants)',
                    color='conference_type'
                  )

class ConversionDateFr():
    
    def type(self):
        return sa.types.Date()

    def to(self, arg):
        return pd.to_datetime(arg, format='%d/%m/%Y', errors='coerce')

class ConversionString():
    
    def type(self):
        return sa.types.String()

    def to(self, arg):
        return arg

class ConversionInteger():
    
    def type(self):
        return sa.types.Integer()

    def to(self, arg):
        return pd.to_numeric(arg, errors='coerce')


class SourceGeneric(ABC):

    @abstractmethod
    def __init__(self, input_base, input_table, output_table):
        self.input_base = input_base
        self.input_table = input_table
        self.output_table = output_table
        self.mapping = []

    @abstractmethod
    def extract(self) -> list:
        pass

    def types(self) -> dict:
        return {new_name: conversion.type() for _, new_name, conversion in self.mapping}

    def transform(self, df) -> pd.DataFrame:
        df = df[[old_name for old_name, _, _ in self.mapping]]
        df = df.rename(columns={old_name: new_name for old_name, new_name, _ in self.mapping})
        df = df.replace(r'^\s*$', None, regex=True)
        for _, new_name, conversion in self.mapping:
            df[new_name] = df[new_name].apply(conversion.to)
        return df


class SourceGsheet(SourceGeneric):

    def extract(self):
        gs = gspread.service_account(filename=Path(f"{os.environ['SERVICE_ACCOUNT_FILE']}"))
        return gs.open(self.input_base).worksheet(self.input_table).get_all_records()


class SourceAirtable(SourceGeneric):

    def extract(self):
        table = Table(f"{os.environ['AIRTABLE_KEY']}", self.input_base, self.input_table)
        return [{'id': r['id']} | r['fields'] for r in table.all()]


class SourceTtsConference(SourceGsheet):

    def __init__(self, input_base, input_table, output_table):
        SourceGeneric.__init__(self, input_base, input_table, output_table)
        self.mapping = [
            ['DATE DE LA CONFERENCE', 'conference_date', ConversionDateFr()],
            ['QUELLE CONFERENCE AVEZ-VOUS DONNEE ?', 'conference_type', ConversionString()],
            ['TYPE DE DEMANDEUR', 'demandeur_type', ConversionString()],
            ['GROUPE LOCAL', 'groupe_local', ConversionString()],
            ['VOTRE ADRESSE EMAIL', 'conferencier_email1', ConversionString()],
            ['ADRESSE MAIL DU SECOND CONFERENCIER', 'conferencier_email2', ConversionString()],
            ['COMBIEN DE PERSONNES ENVIRON ONT ASSISTE A LA CONFERENCE ?', 'nombre_participants', ConversionInteger()]
        ]


class SourceProjInitiative(SourceAirtable):

    def __init__(self, input_base, input_table, output_table):
        SourceGeneric.__init__(self, input_base, input_table, output_table)
        self.mapping = [
        ]


Destination().load([SourceTtsConference("TTS - Conférences passées", "Conférences passées", "tts_conferences")])
#Destination().load([SourceProjInitiative("apptcz10YqVmqSNU6", "Initiatives", "proj_initiatives")])
