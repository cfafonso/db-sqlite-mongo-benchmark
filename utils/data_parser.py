
import os
import json
import pandas as pd


def parse_json_file(file_path, file_name):
    """
    Parses a json file from the specified directory.

    Args:
        file_path (str): path where the json file is located.
        file_name (str): name of the json file to be parsed.

    Returns:
        list: the parsed JSON content as a list of dictionaries.
    """
    
    with open(os.path.join(file_path, file_name)) as file:
        return json.load(file)


def read_process_airlines_data(file_path, file_name):
    """
    Reads airline data from a CSV file and processes it by remaning the 'IATA_CODE' column.

    Args:
        file_path (str): path where the airlines CSV file is located.
        file_name (str): name of the airlines CSV file.
    
    Returns:
        pandas.DataFrame: a processed DataFrame with airline data where the 'IATA_CODE' column has been renamed to
                          'AIRLINE_CODE'.
    """

    df = pd.read_csv(os.path.join(file_path, file_name))
    df.rename(columns = {"IATA_CODE":"AIRLINE_CODE"}, inplace = True)

    return df


def read_process_airports_data(file_path, file_name):
    """
    Reads the airport CSV file from the specified path and reorders its columns.

    Args:
        file_path (str): path where the airports CSV file is located.
        file_name (str): name of the airports CSV file.
    
    Returns:
        pandas.DataFrame: a processed DataFrame with reordered columns where the 6th column and 7th column have been swapped.
    """

    df = pd.read_csv(os.path.join(file_path, file_name))
    df = df.iloc[:, [0,1,2,3,4,6,5]]

    return df


def read_process_flights_data(file_path, file_name, month):
    """
    Reads the flights CSV file and processes it by filtering for a specific month, renaming columns, dropping unnecessary
    columns, and reordering the remaining columns.

    Args:
        file_path (str): path where the flights CSV file is located.
        file_name (str): name of the flights CSV file.
        month (int): month number to filter data (1=January, 2=February, etc.)

    Returns:
        pandas.DataFrame: a processed DataFrame containing flight data for the specified month with renamed, reordered, 
                          and filtered columns.
    """

    df = pd.read_csv((os.path.join(file_path, file_name)), low_memory=False)
    df.rename(columns = {"AIRLINE":"AIRLINE_CODE"}, inplace = True)
    df = df.loc[df["MONTH"] == month]

    df = df.drop(['YEAR', 'MONTH', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'TAXI_OUT','WHEELS_OFF', 'SCHEDULED_TIME', 
                  'ELAPSED_TIME', 'AIR_TIME', 'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME', 'DIVERTED', 
                  'CANCELLED', 'CANCELLATION_REASON', 'AIR_SYSTEM_DELAY', 'SECURITY_DELAY', 'AIRLINE_DELAY', 
                  'LATE_AIRCRAFT_DELAY', 'WEATHER_DELAY'], axis = 1)

    df = df.iloc[:, [0,1,2,3,4,8,5,6,7,9]]
    
    return df