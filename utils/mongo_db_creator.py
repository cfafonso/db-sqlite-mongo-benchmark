
from utils.data_parser import parse_json_file


def create_collections(db, dataset_names, json_file_path):
    """
    Creates the airlines, airports and flights collections in a MongoDB database and populates it with the provided data.

    Args:
        db (pymongo.database.Database): MongoDB database object.
        dataset_names (list): list of collection names to create.
        json_file_path (str): path where the JSON files are located.
    """
    
    collections = {}

    for dataset_name in dataset_names:
        data = parse_json_file(json_file_path, dataset_name + '.json')
        if dataset_name in db.list_collection_names():
            db[dataset_name].drop()
        
        collections[dataset_name] = db.create_collection(dataset_name)
        collections[dataset_name].insert_many(data)