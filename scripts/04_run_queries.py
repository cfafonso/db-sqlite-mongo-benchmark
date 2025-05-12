
import os
import json
import sqlite3

from pymongo import MongoClient
from constants import json_file_path, sqlite_db_path, results_path
from utils.query_helper import query_sqlite_db

########## DATABASE CONNECTIONS ##########

# sqlite3
conn = sqlite3.connect(os.path.join(sqlite_db_path, 'flights_sqlite_database.db'))
c = conn.cursor()


# mongodb
client = MongoClient()
db = client.flights_mongo_database

flights_collection, airports_collection, airlines_collection = db['flights'], db['airports'], db['airlines']


################# QUERIES #################

# query 1:
# select the "flight_number" from the "flights" table or collection where the "flight_number" is greater than 5000
query_1_sqlite_results = query_sqlite_db(c, "SELECT flight_number \
                                             FROM flights \
                                             WHERE flight_number > 5000")

query_1_mongodb_results = flights_collection.find({"flight_number": {"$gt":5000}}, {"flight_number":1})


# query 2:
# select the "airport", "city" pairs from the "airports" table or collection
query_2_sqlite_results = query_sqlite_db(c, "SELECT airport, city \
                                             FROM airports")

query_2_mongodb_results = airports_collection.find({}, {"airport": 1, "city": 1})


# query 3:
# select the "airport", "longitude" and "latitude" from the "airports" table or collection where "latitude" is greater
# than 30 and lower than 50
query_3_sqlite_results = query_sqlite_db(c, "SELECT airport, longitude, latitude \
                                             FROM airports \
                                             WHERE latitude BETWEEN 30 AND 50")

query_3_mongodb_results = airports_collection.find({"coordinates.latitude": {"$gt":30, "$lt": 50}}, {"airport":1, "coordinates": 1})


# query 4:
# select the "flight_id", "origin_airport" (from the "flights" table or collection), "airport" (from the "airports" 
# table or collection and which corresponds to the name of the origin airport of the flight), "airline_code" (from 
# the "flights" table or collection) and "airline" (from the "airlines" table or collection)
query_4_sqlite_results = query_sqlite_db(c, "SELECT f.flight_id, f.origin_airport, apt.airport, f.airline_code, air.airline \
                                             FROM flights f \
                                             JOIN airports apt ON (f.origin_airport = apt.iata_code) \
                                             JOIN airlines air ON (f.airline_code = air.airline_code)")


query_4_mongodb_results = flights_collection.aggregate(pipeline = [{"$lookup": {"from": "airports", 
                                                                                "localField": "iata_code.origin_airport", 
                                                                                "foreignField": "iata_code", 
                                                                                "as": "airport_info"}},
                                                                   {"$lookup": {"from": "airlines", 
                                                                                "localField": "airline_code", 
                                                                                "foreignField": "airline_code", 
                                                                                "as": "airline_info"}},
                                                                   {"$unwind": "$airport_info"},
                                                                   {"$unwind": "$airline_info"},
                                                                   {"$project": {"flight_id": 1, 
                                                                                 "origin_airport": "$iata_code.origin_airport", 
                                                                                 "airport": "$airport_info.airport", 
                                                                                 "airline_code": 1, 
                                                                                 "airline": "$airline_info.airline"}}])


# query 5
# update the "airports" table or collection, setting the "longitude" equal to -75 where "iata_code" is "ABE"
query_5_sqlite_results_before_insert = query_sqlite_db(c, "SELECT * \
                                                           FROM airports \
                                                           WHERE iata_code = 'ABE'")
c.execute("UPDATE airports \
           SET longitude = -75 \
           WHERE iata_code = 'ABE'")

query_5_sqlite_results_after_insert = query_sqlite_db(c, "SELECT * \
                                                          FROM airports \
                                                          WHERE iata_code = 'ABE'")

query_5_mongodb_results_before_insert = airports_collection.find({"iata_code": "ABE"})
airports_collection.update_many({"iata_code": "ABE"}, {"$set": {"coordinates.longitude": -75}})
query_5_mongodb_results_after_insert = airports_collection.find({"iata_code": "ABE"})


# query 6:
# insert into the "airlines" table or collection two new airlines ("CA", "Celestial Airways"), ("AW", "Azure Wings")

## sqlite3
c.execute("INSERT INTO airlines (airline_code, airline) \
           VALUES ('CA', 'Celestial Airways'), \
                  ('AW', 'Azure Wings')")

query_6_sqlite_results_after_insert = query_sqlite_db(c, "SELECT * \
                                                          FROM AIRLINES \
                                                          WHERE airline_code in ('CA', 'AW')")

## mongodb
with open(os.path.join(json_file_path, 'airlines_insert.json')) as file:
    data_insert = json.load(file)

airlines_collection.insert_many(data_insert)
query_6_mongodb_results = airlines_collection.find({"airline_code": {"$in": ["CA", "AW"]}}, {"_id": 0})


# query 7: 
# insert a new record in the "flights" table or collection

## sqlite3
c.execute("INSERT INTO FLIGHTS (day, day_of_the_week, airline_code, flight_number, tail_number, distance, \
                                origin_airport, destination_airport, departure_delay, arrival_delay) \
           VALUES (1, 2, 'CA', 90, 'N405AS', 1000, 'ANC', 'SEA', 10.0, -8.0)")

query_7_sqlite_results_after_insert = query_sqlite_db(c, "SELECT * \
                                                          FROM flights \
                                                          WHERE airline_code = 'CA'")

## mongodb
new_flight = {"flight_id": 469969, 
              "day":1, 
              "day_of_the_week":2, 
              "airline_code": "CA", 
              "flight_number":90, 
              "tail_number":"N405AS", 
              "distance":1000, 
              "iata_code":{"origin_airport":"ANC", "destination_airport":"SEA"},
              "delays":{"departure_delay":10.0, "arrival_delay":-8.0}}

query_7_mongodb = flights_collection.insert_one(new_flight)
new_mongodb_record = flights_collection.find_one({"_id": query_7_mongodb.inserted_id}, {"_id": 0})


########### SAVING THE RESULTS ############

os.makedirs(results_path, exist_ok=True)

with open(os.path.join(results_path, 'queries.txt'), 'w+') as f:
    f.write('-----------\n')
    f.write('Query 1: Select the "flight_number" from the "flights" table or collection where the "flight_number" is greater than 5000.\n')
    f.write('\n# The first five results for the 1st query in SQLite are:\n')
    f.write('\n'.join(f"{record[0]}" for record in query_1_sqlite_results[:5]))
    f.write('\n\n# The first five results for the 1st query in MongoDB are:\n')
    f.write('\n'.join(f"{doc['flight_number']}" for doc in query_1_mongodb_results[:5]))
    
    f.write('\n-----------')

    f.write('\nQuery 2: Select the "airport", "city" pairs from the "airports" table or collection.\n')
    f.write('\n# The first five results for the 2nd query in SQLite are:\n')
    f.write("\n".join(', '.join(str(item) for item in record) for record in query_2_sqlite_results[:5]))
    f.write('\n\n# The first five results for the 2nd query in MongoDB are:\n')
    f.write('\n'.join(f"{doc['airport']}, {doc['city']}" for doc in query_2_mongodb_results[:5]))

    f.write('\n-----------')

    f.write('\nQuery 3: Select the "airport", "longitude" and "latitude" from the "airports" table or collection where "latitude" is greater than 30 and lower than 50.\n')
    f.write('\n# The first five results for the 3rd query in SQLite are:\n')
    f.write("\n".join(', '.join(str(item) for item in record) for record in query_3_sqlite_results[:5]))
    f.write('\n\n# The first five results for the 3rd query in MongoDB are:\n')
    f.write('\n'.join(f"{doc['airport']}, {doc['coordinates']['longitude']}, {doc['coordinates']['latitude']}" for doc in query_3_mongodb_results[:5]))

    f.write('\n-----------')
    
    f.write('\nQuery 4: Select the "flight_id", "origin_airport" (from the "flights" table or collection), "airport" (from the "airports" table or collection and which corresponds to the name of the origin airport of the flight), "airline_code" (from the "flights" table or collection) and "airline" (from the "airlines" table or collection).\n')
    f.write('\n# The first five results for the 4th query in SQLite are:\n')
    f.write('\n'.join(', '.join(str(item) for item in record) for record in query_4_sqlite_results[:5]))
    f.write('\n\n# The first five results for the 4th query in MongoDB are:\n')
    f.write('\n'.join(f"{doc['flight_id']}, {doc['origin_airport']}, {doc['airport']}, {doc['airline_code']}, {doc['airline']}" for doc in list(query_4_mongodb_results)[:5]))

    f.write('\n-----------')

    f.write('\nQuery 5: Update the "airports" table or collection, setting the "longitude" equal to -75 where "iata_code" is "ABE".\n')
    f.write('\n# Previous results in the "airports" table in SQLite:\n')
    f.write('\n'.join(', '.join(str(item) for item in record) for record in query_5_sqlite_results_before_insert[:5]))
    f.write('\n# Updated results in the "airports" table in SQLite:\n')
    f.write('\n'.join(', '.join(str(item) for item in record) for record in query_5_sqlite_results_after_insert[:5]))

    f.write('\n\n# Previous results in the "airports" collection in MongoDB:\n')
    f.write('\n'.join(f"{doc['iata_code']}, {doc['airport']}, {doc['city']}, {doc['state']}, {doc['country']}, {doc['coordinates']['longitude']}, {doc['coordinates']['latitude']}" for doc in list(query_5_mongodb_results_before_insert)[:5]))
    f.write('\n# Updated results in the "airports" collection in MongoDB:\n')   
    f.write('\n'.join(f"{doc['iata_code']}, {doc['airport']}, {doc['city']}, {doc['state']}, {doc['country']}, {doc['coordinates']['longitude']}, {doc['coordinates']['latitude']}" for doc in list(query_5_mongodb_results_after_insert)[:5]))

    f.write('\n-----------')

    f.write('\nQuery 6: Insert into the "airlines" table or collection two new airlines ("CA", "Celestial Airways"), ("AW", "Azure Wings").\n')
    f.write('\nThe added record for the 7th query in the "airlines" table in SQLite is:\n')
    f.write('\n'.join(', '.join(str(item) for item in record) for record in query_6_sqlite_results_after_insert[:5]))
    f.write('\n\nThe added record for the 7th query in the "airlines" collection is:\n')
    f.write('\n'.join(f"{doc['airline_code']}, {doc['airline']}" for doc in query_6_mongodb_results))

    f.write('\n-----------')

    f.write('\nQuery 7: Insert a new record in the "flights" table or collection.\n')
    f.write('\nThe newly inserted record in the "flights" table in SQLite is:\n')
    f.write('\n'.join(', '.join(str(item) for item in record) for record in query_7_sqlite_results_after_insert))
    f.write('\n\nThe newly inserted record in the "flights" collection in MongoDB is:\n')
    f.write(f"{new_mongodb_record['flight_id']}, {new_mongodb_record['day']}, {new_mongodb_record['day_of_the_week']}, "
            f"{new_mongodb_record['airline_code']}, {new_mongodb_record['flight_number']}, "
            f"{new_mongodb_record['tail_number']}, {new_mongodb_record['distance']}, "
            f"{new_mongodb_record['iata_code']['origin_airport']}, "
            f"{new_mongodb_record['iata_code']['destination_airport']}, "
            f"{new_mongodb_record['delays']['departure_delay']:g}, {new_mongodb_record['delays']['arrival_delay']:g}")
    
    f.write('\n-----------')