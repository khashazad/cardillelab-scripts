import csv

from Mongo.MongoAdapterV2 import MongoDriver
from pymongo.errors import OperationFailure

mongo = MongoDriver(local=True)


def generate_report():
    report_file_name = "missing-lakes-per-fish-id.csv"

    with open("./Assets/inventory.csv") as inventory_file:
        inventory = csv.reader(inventory_file)
        next(inventory)

        with open(report_file_name, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "Fishnet",
                    "Fish_Id",
                    "Total_Lakes",
                    "Exported_Lakes",
                    "Percentage_Missing",
                ]
            )

            for asset in inventory:
                fishnet = asset[0]
                fish_id = asset[1]
                lakes_count = int(asset[2])

                collection = f"c{fishnet}_l8_{fish_id}"

                available_lakes_count = len(
                    list(mongo.db[collection].find({}).distinct("hylak_id"))
                )

                missing_lakes_count = lakes_count - available_lakes_count

                percentage_lakes_missing = missing_lakes_count * 100 / lakes_count

                writer.writerow(
                    [
                        fishnet,
                        fish_id,
                        lakes_count,
                        missing_lakes_count,
                        percentage_lakes_missing,
                    ]
                )

    print("CSV report generated successfully.")


if __name__ == "__main__":
    generate_report()
