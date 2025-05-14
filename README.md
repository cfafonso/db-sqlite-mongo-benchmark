# db-sqlite-mongo-benchmark
This tool provides a comparative analysis of SQLite (relational) and MongoDB (document-oriented) databases. It covers database creation, querying and indexing. The dataset is related to flight delays and cancellations in the United States of America during the year 2015 and can be found [here](https://www.kaggle.com/datasets/usdot/flight-delays).


## Prerequisites

- Python 3.x
- MongoDB

## Installation

### 1. Clone or download the repository

```git clone https://github.com/cfafonso/db-sqlite-mongo-benchmark.git```


### 2. Ensure MongoDB is running on your system.

### 3. Download the dataset

Go [here](https://www.kaggle.com/datasets/usdot/flight-delays), download the `airlines.csv`, `airports.csv` and `flights.csv` files and place them inside the `data/datasets` folder.


### 4. Install the required packages:

```pip install -r requirements.txt```


### 5. Usage

Inside the `db-sqlite-mongo-benchmark` folder, run the following files or the batch file ```db_sqlite_mongo_benchmark.bat```:

```
cd db-sqlite-mongo-benchmark
db_sqlite_mongo_benchmark.bat
```

or

```
cd db-sqlite-mongo-benchmark
python -m scripts.01_create_sqlite_databases
python -m scripts.02_convert_csv_to_json
python -m scripts.03_create_mongo_databases
python -m scripts.04_run_queries
python -m scripts.05_create_indexes
python -m scripts.06_compare_query_performance
```


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## Structure
```
db-sqlite-mongo-benchmark/
├── data/
    ├── datasets/                               # The folder to store the three dataset CSV files
    ├── json_files/                             # The folder to store the JSON files
        └── airlines_insert.json                # JSON file to insert data into the airlines collection
    └── sqlite_database_files/
        ├── flights_sqlite_database_index.db    # SQLite database file for indexing purposes
        └── flights_sqlite_database.db          # SQLite database file
├── results/
    ├── mongo_db_performance.png                # Figure to show performance with and without indexing in MongoDB
    ├── queries.txt                             # Results of the queries
    └── sqlite_db_performance.png               # Figure to show performance with and without indexing in SQLite
├── schemas/
    ├── mongo_db_schema.png                     # Figure of the database schema in MongoDB
    └── sqlite_db_schema.png                    # Figure of the database schema in SQL
├── scripts/
    ├── 01_create_sqlite_databases.py           # Creates the SQLite databases according to the schema
    ├── 02_convert_csv_to_json.py               # Converts the dataset CSV files to JSON ones according to the database schema
    ├── 03_create_mongo_databases.py            # Creates the MongoDB databases according to the schema
    ├── 04_run_queries.py                       # Performs queries in the two non-indexed databases in SQLite and MongoDB
    ├── 05_create_indexes.py                    # Implements indexing in two databases in SQLite and MongoDB
    └── 06_compare_query_performance.py         # Compares query performance with and without indexing
├── utils/
    ├── data_parser.py                          # Utility functions to read and process files
    ├── mongo_db_creator.py                     # Utility functions to create the MongoDB databases
    ├── performance_analysis.py                 # Utility functions to compare query execution times
    ├── query_helper.py                         # Utility function to query the SQLite database
    └── sqlite_db_creator.py                    # Utility functions to create the SQLite databases
├── LICENSE                                     # License
├── README.md                                   # Project documentation
├── constants.py                                # File containing fixed values that remain unchanged throughout execution
├── db_sqlite_mongo_benchmark.bat               # Windows batch file to run the db-sqlite-mongo-benchmark tool
└── requirements.txt                            # Python package dependencies needed to run the project.
