
import os
import sqlite3

from pymongo import MongoClient
from constants import sqlite_db_path


########## DATABASE CONNECTIONS ##########

# MongoDB

## Without indexing
client = MongoClient()
db = client.flights_mongo_database

airlines_collection, airports_collection, flights_collection = db['airlines'], db['airports'], db['flights']

## With indexing
client_index = MongoClient()
db_index = client_index.flights_mongo_database_index

airlines_collection_index, airports_collection_index, flights_collection_index = db_index['airlines'], db_index['airports'], db_index['flights']

# SQLite

## Without indexing
conn = sqlite3.connect(os.path.join(sqlite_db_path, 'flights_sqlite_database.db'))
c = conn.cursor()

# With indexing
conn_index = sqlite3.connect(os.path.join(sqlite_db_path, 'flights_sqlite_database_index.db'))
c_index = conn_index.cursor()


########## CREATION OF INDEXES ##########

# MongoDB

## hash index on flight_number
mongo_index_1 = flights_collection_index.create_index([("flight_number", 'hashed')])

## compound text index on airport and city
mongo_index_2 = airports_collection_index.create_index([("airport", 'text'), ("CITY", 'text')])

## geospatial index for coordinates
mongo_index_3 = airports_collection_index.create_index([("coordinates", '2dsphere')])

# SQLite

## index on flight_number
createIndex = "CREATE INDEX index_flights \
               ON flights (flight_number)"
c_index.execute(createIndex)

## compound text index on airport and city
multi_col = "CREATE INDEX air_city \
             ON airports (airport, city);"
c_index.execute(multi_col)

## compound index for coordinates
multi_col = "CREATE INDEX coordinates \
             ON airports (latitude, longitude);"
c_index.execute(multi_col)