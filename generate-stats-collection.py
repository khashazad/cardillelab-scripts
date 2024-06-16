from os import stat
from Services.LoggerV2 import Logger
from Mongo.MongoAdapterV2 import MongoDriver
import csv

PATH_INVENTORY = r"Assets/inventory.csv"

logger = Logger("generate-stats", format=False)
mongo = MongoDriver(logger, local=True)


def generate_lookup_collection():
    # list of lookup objects that will be inserted to the database collection
    stats_records = []

    try:
        with open(PATH_INVENTORY) as csv_file:
            reader = csv.reader(csv_file, delimiter=",")
            next(reader)  # skip header

            for asset in reader:
                if asset[0] == "" or asset[1] == "":
                    continue

                collection_id = asset[0]
                asset_id = asset[1]
                total_lakes_count = asset[2]

                stats_records.append(
                    {
                        "collection": int(collection_id),
                        "asset_id": int(float(asset_id)),
                        "lakes_count": int(total_lakes_count),
                    }
                )

        mongo.insert_many("asset_stats", stats_records)

    except Exception as error:
        logger.log_error(error)


if __name__ == "__main__":
    generate_lookup_collection()
