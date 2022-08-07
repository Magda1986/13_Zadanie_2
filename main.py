# po instalacji SQLalchemy (pip instal sqlalchemy==1.3.16) importujemy sqlalchemy i csv

import sqlalchemy
import csv

# zgodnie ze skryptem importujemy koljne moduły z sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from sqlalchemy import create_engine
# Import modułu obsługującego datę 
from datetime import date

engine = create_engine("sqlite:///database.db", echo=True)
meta = MetaData()

#tworzymy funkcję odpowiedzialną odczyt plików CSV
def load_items_csv(csvfile):
    datas = []
    with open(csvfile, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter= ",")
        for row in reader:
            datas.append(row)
            return datas

stations_datas = load_items_csv("clean_stations.csv")
measure_datas = load_items_csv("clean_measure.csv")

# definiujemy tabelę clean_stations
stations = Table(
    "stations",
    meta,
    Column("station", String, primary_key=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String)
)

# definiujemy tabelę clean_measure
measure = Table(
    "measure", meta,
    Column("station", String),
    Column("date", date),
    Column("precip", Float),
    Column("tobs", Integer)
)

stations_datas_to_insert = stations.insert().values(stations_datas)
measure_datas_to_insert = measure.insert().values(measure_datas)

conn = engine.connect()
conn.execute(measure_datas_to_insert)
conn.execute(stations_datas_to_insert)

conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
conn.execute("SELECT * FROM measure LIMIT 5").fetchall()