from Mongo.MongoAdapter import MongoDriver
from Services.LoggerService import LoggerService as Logger
import csv

PATH_INVENTORY  = r"Assets/inventory.csv"

def generate_lookup_collection():
    # list of lookup objects that will be inserted to the database collection
    lookup_records = []

    try:
        with open(PATH_INVENTORY) as csv_file:
            reader = csv.reader(csv_file, delimiter=",")
            next(reader) # skip header

            for asset in reader:
                if asset[0] == "" or asset[1] == "":
                    continue
                        
                collection_id = asset[0]
                asset_id = asset[1]

                asset_file_path = r"Assets/Fishnet{}/fish_ID{}.csv".format(collection_id, asset_id)
                
                with open(asset_file_path, "r") as file:
                    reader = csv.reader(file)
                    next(reader)  # skip header

                    for row in reader:
                        lookup_records.append(
                            {
                                "collection": int(collection_id),
                                "asset_id": int(float(asset_id)),
                                "hylak_id": int(float(row[0])),
                                "longitude": float(row[1]),
                                "latitude": float(row[2]),
                            }
                        )

        MongoDriver.insert_many_reset_collection("lookup", lookup_records)

        Logger.log_info("Inserted all lookup data")

    except Exception as error:
        Logger.log_error(error)


if __name__ == "__main__":
    generate_lookup_collection()