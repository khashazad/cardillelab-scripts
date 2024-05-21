import os
import csv
from Mongo.MongoAdapterV2 import MongoDriver
from pymongo.errors import OperationFailure

lakes_inventory_path = os.path.abspath("./Assets/inventory.csv")

report_file = os.path.abspath("missing-lakes-stats.csv")

mongo = MongoDriver(local=True)


def check_data_existence_for_fish_id(fishnet, fish_id, report_writer):
    collection = f"c{fishnet}_l8_{fish_id}"

    asset_file_path = f"Assets/Fishnet{fishnet}/fish_ID{fish_id}.csv"

    with open(asset_file_path, "r") as file:
        lakes = csv.reader(file)
        next(lakes)  # skip header

        try:
            mongo.db.validate_collection(collection)

            records = list(mongo.db[collection].find({}))

            for lake in lakes:
                hylak_id = lake[2]

                if hylak_id not in [None, ""]:
                    data_available = True in (
                        data["hylak_id"] == int(lake[2]) for data in records
                    )

                    if not data_available:
                        report_writer.writerow([fishnet, fish_id, hylak_id])

        except OperationFailure:
            for lake in lakes:
                hylak_id = lake[2]

                if hylak_id not in [None, ""]:
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

                check_data_existence_for_fish_id(fish_id, fish_id, report_writer)

                print(f"Fishnet {fishnet} Fish Id {fish_id} stats retrieved")

        print("Finished")
