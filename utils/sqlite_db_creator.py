
from utils.data_parser import read_process_airlines_data, read_process_airports_data, read_process_flights_data


def create_table_airlines(db, cur, data):
    """
    Creates the airlines table in an SQLite database and populates it with the provided data. If the table already
    exists, it will be dropped first and then recreated.

    Args:
        db (sqlite3.Connection): SQLite database connection object.
        cur (sqlite3.Cursor): SQLite cursor object for executing SQL statements.
        data (list): list of tuples containing the airlines data in the format (airline_code, airline).
        indexing (bool, optional): flag to indicate whether this table is being created for indexing purposes. Defaults
                                   to False.
    """

    cur.execute('DROP TABLE IF EXISTS airlines')
    cur.execute('''CREATE TABLE airlines (
                    airline_code        TEXT(2)         PRIMARY KEY, 
                    airline             TEXT(100)       NOT NULL);''')

    cur.executemany("INSERT INTO airlines (airline_code, airline) \
                     VALUES(?, ?)", data)
    db.commit()


def create_table_airports(db, cur, data):
    """
    Creates the airports table in an SQLite database and populates it with the provided data. If the table already
    exists, it will be dropped first and then recreated.

    Args:
        db (sqlite3.Connection): SQLite database connection object.
        cur (sqlite3.Cursor): SQLite cursor object for executing SQL statements.
        data (list): list of tuples containing the airports data in the format (iata_code, airport, city, state, country,
                     longitude, latitude).
        indexing (bool, optional): flag to indicate whether this table is being created for indexing purposes. Defaults
                                   to False.
    """

    cur.execute('DROP TABLE IF EXISTS airports')
    cur.execute('''CREATE TABLE airports (
                    iata_code       TEXT(3)         PRIMARY KEY, 
                    airport         TEXT(100)       NOT NULL,
                    city            TEXT(50)        NOT NULL,
                    state           TEXT(2)         NOT NULL,
                    country         TEXT(3)         NOT NULL,
                    longitude       FLOAT,
                    latitude        FLOAT);''')

    cur.executemany('''INSERT INTO airports (iata_code, airport, city, state, country, longitude, latitude)
                     VALUES(?, ?, ?, ?, ?, ?, ?)''', data)
    db.commit()


def create_table_flights(db, cur, data):
    """
    Creates the flights table in an SQLite database and populates it with the provided data. If the table already
    exists, it will be dropped first and then recreated.

    Args:
        db (sqlite3.Connection): SQLite database connection object.
        cur (sqlite3.Cursor): SQLite cursor object for executing SQL statements.
        data (list): list of tuples containing the flight data in the format (day, day_of_the_week, airline_code, 
                     flight_number, tail_number, distance, origin_airport, destination_airport, departure_delay, 
                     arrival_delay).
        indexing (bool, optional): flag to indicate whether this table is being created for indexing purposes. Defaults
                                   to False.
        
    Note:
        - The flights table has foreign key constraints that reference the airports and airlines tables.
        - The function expects the airports table to already exist with an iata_code column.
        - The function expects the airlines table to already exist with an airline_code column.
    """

    cur.execute('''DROP TABLE IF EXISTS flights''')
    cur.execute('''CREATE TABLE flights (
                    flight_id              INTEGER         PRIMARY KEY AUTOINCREMENT,
                    day                    INTEGER(2)      NOT NULL,
                    day_of_the_week        INTEGER(1)      NOT NULL,
                    airline_code           TEXT(2)         NOT NULL,
                    flight_number          INTEGER(4)      NOT NULL,
                    tail_number            TEXT(6),
                    distance               INTEGER(4)      NOT NULL,
                    origin_airport         TEXT(3)         NOT NULL,
                    destination_airport    TEXT(3)         NOT NULL,
                    departure_delay        INTEGER(4),
                    arrival_delay          INTEGER(4),
                  --
                    FOREIGN KEY (origin_airport) 
                    REFERENCES airports (iata_code) 
                    ON UPDATE CASCADE ON DELETE CASCADE,
                  --
                    FOREIGN KEY (destination_airport) 
                    REFERENCES airports (iata_code) 
                    ON UPDATE CASCADE ON DELETE CASCADE,
                  --
                    FOREIGN KEY (airline_code) 
                    REFERENCES airlines (airline_code) 
                    ON UPDATE CASCADE ON DELETE CASCADE);''')

    cur.executemany('''INSERT INTO flights (day, day_of_the_week, airline_code, flight_number, tail_number, distance, 
                                            origin_airport, destination_airport, departure_delay, arrival_delay) 
                       VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    db.commit()


def create_tables(db, cur, datasets_path, dataset_names):
    """
    Creates the airlines, airports and flights tables in an SQLite database and populates it with the provided data.

    Args:
        db (sqlite3.Connection): SQLite database connection object.
        cur (sqlite3.Cursor): SQLite cursor object for executing SQL statements.
        indexing (bool, optional): flag to indicate whether this table is being created for indexing purposes. Defaults
                                   to False.
    """

    for dataset in dataset_names:

        if dataset == 'airlines':
            df = read_process_airlines_data(datasets_path, dataset + '.csv')
            data = list(df.itertuples(index = False, name = None))
            create_table_airlines(db, cur, data)
        
        elif dataset == 'airports':
            df = read_process_airports_data(datasets_path, dataset + '.csv')
            data = list(df.itertuples(index = False, name = None))
            create_table_airports(db, cur, data)
        
        elif dataset == 'flights':
            df = read_process_flights_data(datasets_path, dataset + '.csv', 1)
            data = list(df.itertuples(index = False, name = None))
            create_table_flights(db, cur, data)

    db.close()