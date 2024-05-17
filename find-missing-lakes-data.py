from Services.Logger import Logger
from Mongo.MongoAdapterV2 import MongoDriver
import pymongo
import os
import csv

logger = Logger("missing_lakes_info.log", "missing_lakes_error.log", False)
mongo = MongoDriver(logger)

lakes_inventory_path = os.path.abspath("./Assets/inventory.csv")


def count_available_lakes_data(fishnet, fish_id, lakes_count):
    collection = f"c{fishnet}_l8_{fish_id}"

    try:
        mongo.db.validate_collection(collection)

        available_lakes_count = len(mongo.db[collection].find({}).distinct("hylak_id"))

        logger.log_info(
            f"{fishnet},{fish_id},{lakes_count},{available_lakes_count},{int(int(lakes_count) - available_lakes_count)}"
        )

    except pymongo.errors.OperationFailure:
        logger.log_error(f"missing collection for asset {fish_id} of fishnet {fishnet}")

        logger.log_info(f"{fishnet},{fish_id},{lakes_count},0,{lakes_count}")


if __name__ == "__main__":
    with open(lakes_inventory_path, mode="r") as file:
        csv_reader = csv.reader(file)

        next(csv_reader)

        headers = "fishnet,fishID,total_lakes,available_lakes,missing_lakes"
        logger.log_info(headers)

        for row in csv_reader:
            count_available_lakes_data(row[0], row[1], row[2])
