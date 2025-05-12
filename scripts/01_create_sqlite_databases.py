
import os
import sqlite3

from constants import sqlite_db_path, datasets_path, dataset_names

from utils.sqlite_db_creator import create_tables


################# CREATION OF THE DATABASES #################

# create the folder containing the database files
os.makedirs(sqlite_db_path, exist_ok=True)

# create the database without indexing
db = sqlite3.connect(os.path.join(sqlite_db_path, 'flights_sqlite_database.db'))
cur = db.cursor()

# create the database with indexing
db_index = sqlite3.connect(os.path.join(sqlite_db_path, 'flights_sqlite_database_index.db'))
cur_index = db_index.cursor()

############## DATA LOADING AND TABLE CREATION ##############

### without indexing
create_tables(db, cur, datasets_path, dataset_names)

### with indexing
create_tables(db_index, cur_index, datasets_path, dataset_names)