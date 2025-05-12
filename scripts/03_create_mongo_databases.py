
from pymongo import MongoClient
from constants import dataset_names, json_file_path
from utils.mongo_db_creator import create_collections


################# CREATION OF THE DATABASES #################

# connection
client = MongoClient()

# create the database without indexing
db = client.flights_mongo_database
create_collections(db, dataset_names, json_file_path)

# create the database with indexing
db_index = client.flights_mongo_database_index
create_collections(db_index, dataset_names, json_file_path)