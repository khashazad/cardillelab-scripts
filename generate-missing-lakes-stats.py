import os
import csv
from Mongo.MongoAdapterV2 import MongoDriver
from pymongo.errors import OperationFailure

lakes_inventory_path = os.path.abspath("./Assets/inventory.csv")

report_file = os.path.abspath("missing-lakes-stats.csv")

mongo = MongoDriver(local=True)


def check_data_existence_for_lake(lake, report_writer):
    fishnet = lake[0]
    fish_id = lake[1]
    hylak_id = int(lake[2])

    collection = f"c{fishnet}_l8_{fish_id}"

    try:
        mongo.db.validate_collection(collection)

        lake_data_size = len(
            list(mongo.db[collection].find({"hylak_id": {"eq": hylak_id}}))
        )

        if lake_data_size == 0:
            report_writer.writerow([fishnet, fish_id, hylak_id])

    except OperationFailure:
        report_writer.writerow([fishnet, fish_id, hylak_id])


if __name__ == "__main__":
    with open(lakes_inventory_path, mode="r") as csv_file:
        lakes = csv.reader(csv_file, delimiter=",")

        # skip header
        next(lakes)

        with open(report_file, mode="w") as report:
            report_writer = csv.writer(report)
            for lake in lakes:
                if len(lake) != 0:
                    check_data_existence_for_lake(lake, report_writer)
