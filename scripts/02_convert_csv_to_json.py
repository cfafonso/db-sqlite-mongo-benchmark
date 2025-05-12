
import os

from constants import datasets_path, json_file_path
from utils.data_parser import read_process_airlines_data, read_process_airports_data, read_process_flights_data


# create the folder containing the database files
os.makedirs(json_file_path, exist_ok=True)


########################## AIRLINES #########################

airlines_df = read_process_airlines_data(datasets_path, 'airlines.csv')
airlines_df.columns = airlines_df.columns.str.lower()

airlines_df.to_json(os.path.join(json_file_path, "airlines.json"), orient = "records", date_format = "epoch", 
                    double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

########################## AIRPORTS #########################

airports_df = read_process_airports_data(datasets_path, 'airports.csv')
airports_df.columns = airports_df.columns.str.lower()

airports_df = airports_df[airports_df["longitude"].notna()]
airports_df = airports_df[airports_df["latitude"].notna()]

airports_df["coordinates"] = airports_df[["longitude", "latitude"]].apply(lambda s: s.to_list(), axis = 1)
airports_df["coordinates"] = airports_df["coordinates"].apply(lambda loc: {"longitude": loc[0], "latitude": loc[1]})
airports_df.drop(["longitude", "latitude"], axis = 1).to_json(os.path.join(json_file_path, "airports.json"), 
                                                              orient = "records", date_format = "epoch", double_precision = 10, 
                                                              force_ascii = True, date_unit = "ms", default_handler = None, indent=2)


########################## FLIGHTS ##########################

flights_df = read_process_flights_data(datasets_path, 'flights.csv', 1)
flights_df.columns = flights_df.columns.str.lower()
flights_df.insert(0, 'flight_id', range(1, len(flights_df) + 1))

flights_df["iata_code"] = flights_df[["origin_airport", "destination_airport"]].apply(lambda s: s.to_dict(), axis = 1)
flights_df["delays"] = flights_df[["departure_delay", "arrival_delay"]].apply(lambda s: s.to_dict(), axis = 1)

flights_df.drop(["origin_airport", "destination_airport", "departure_delay", "arrival_delay"], 
                axis = 1).to_json(os.path.join(json_file_path, "flights.json"), orient = "records", date_format = "epoch", 
                                  double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)