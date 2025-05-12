
import os
import sqlite3
import matplotlib.pyplot as plt

from pymongo import MongoClient
from constants import sqlite_db_path, results_path
from utils.performance_analyzer import measure_sqlite_query_time, measure_mongo_query_time, create_performance_plot


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


######## PERFORMANCE EVALUATION #########

labels = ['Query1', 'Query2', 'Query3']

flight_mongo_database_times = []
flight_mongo_database_index_times = []

# MongoDB

## Query 1: Select the "FLIGHT_NUMBER" from the "flights" table or collection where the "flight_number" is greater than 5000.')

flight_mongo_database_times.append(measure_mongo_query_time(flights_collection, {"flight_number": {"$gt":5000}}, {"flight_number":1}))
flight_mongo_database_index_times.append(measure_mongo_query_time(flights_collection_index, {"flight_number": {"$gt":5000}}, {"flight_number":1}))

## Query 2: Select the "airport","city" pairs from the "airports" table or collection.')

flight_mongo_database_times.append(measure_mongo_query_time(airports_collection, {}, {"airport": 1, "CITY": 1}))
flight_mongo_database_index_times.append(measure_mongo_query_time(airports_collection_index, {}, {"airport": 1, "CITY": 1}))

## Query 3: Select the "airport", "latitude" and "longitude" from the "airports" table or collection where "latitude" is greater than 30 and lower than 50.')

flight_mongo_database_times.append(measure_mongo_query_time(airports_collection, {"coordinates.latitude": {"$gt": 30, "$lt": 50}}, 
                                                                           {"airport":1, "coordinates.latitude": 1, "coordinates.longitude": 1}))
flight_mongo_database_index_times.append(measure_mongo_query_time(airports_collection_index, {"coordinates.latitude": {"$gt": 30, "$lt": 50}}, 
                                                                                       {"airport":1, "coordinates.latitude": 1, "coordinates.longitude": 1}))

## Plots

create_performance_plot(flight_mongo_database_times, flight_mongo_database_index_times, labels,
                        'Comparison of query execution times in MongoDB with and without indexing',
                        results_path, 'mongo_db_performance.png')

client.close()
client_index.close()

# SQLite

flight_sqlite_database_times = []
flight_sqlite_database_index_times = []

## Query 1: Select the "FLIGHT_NUMBER" from the "flights" table or collection where the "flight_number" is greater than 5000.')

sqlite_query_1 = "SELECT flight_number \
                  FROM flights \
                  WHERE flight_number > 5000"

## Query 2: Select the "airport","city" pairs from the "airports" table or collection.')
sqlite_query_2 = "SELECT airport, city \
                  FROM airports"

## Query 3: Select the "airport", "latitude" and "longitude" from the "airports" table or collection where "latitude" is greater than 30 and lower than 50.')
sqlite_query_3 = "SELECT airport, latitude, longitude \
                  FROM airports \
                  WHERE latitude BETWEEN 30 AND 50"

flight_sqlite_database_times.append(measure_sqlite_query_time(c, sqlite_query_1))
flight_sqlite_database_index_times.append(measure_sqlite_query_time(c, sqlite_query_1))

flight_sqlite_database_times.append(measure_sqlite_query_time(c, sqlite_query_2))
flight_sqlite_database_index_times.append(measure_sqlite_query_time(c, sqlite_query_2))

flight_sqlite_database_times.append(measure_sqlite_query_time(c, sqlite_query_3))
flight_sqlite_database_index_times.append(measure_sqlite_query_time(c, sqlite_query_3))

## Plots

create_performance_plot(flight_sqlite_database_times, flight_sqlite_database_index_times, labels,
                        'Comparison of query execution times in SQLite with and without indexing',
                        results_path, 'sqlite_db_performance.png')

plt.show()

conn.close()
conn_index.close()