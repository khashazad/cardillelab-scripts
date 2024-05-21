import os
import csv
from typing import assert_type
from Mongo.MongoAdapterV2 import MongoDriver
from pymongo.errors import OperationFailure

lakes_inventory_path = os.path.abspath("./Assets/inventory.csv")

report_file = os.path.abspath("missing-lakes-stats.csv")

mongo = MongoDriver(local=True)


def check_data_existence_for_lake(fishnet, fish_id, hylak_id, report_writer):
    collection = f"c{fishnet}_l8_{fish_id}"

    try:
        mongo.db.validate_collection(collection)

        lake_data_size = len(
            list(mongo.db[collection].find({"hylak_id": {"eq": int(hylak_id)}}))
        )

        if lake_data_size == 0:
            report_writer.writerow([fishnet, fish_id, hylak_id])

    except OperationFailure:
        report_writer.writerow([fishnet, fish_id, hylak_id])


if __name__ == "__main__":
    with open(lakes_inventory_path, mode="r") as csv_file:
        assets = csv.reader(csv_file, delimiter=",")

        # skip header
        next(assets)

        with open(report_file, mode="w") as report:
            report_writer = csv.writer(report)

            for asset in assets:
                if asset[0] == "" or asset[1] == "":
                    continue

                fishnet = asset[0]
                fish_id = asset[1]

                asset_file_path = f"Assets/Fishnet{fishnet}/fish_ID{fish_id}.csv"

                with open(asset_file_path, "r") as file:
                    reader = csv.reader(file)
                    next(reader)  # skip header

                    for lake in reader:
                        if len(lake) != 0:
                            check_data_existence_for_lake(
                                fishnet, fish_id, lake[0], report_writer
                            )
