from Services.LoggerV2 import Logger
from Mongo.MongoAdapterV2 import MongoDriver
import os
import csv
import re
from utils.utils import (
    get_collection_from_fishnet,
    get_asset_file_regex,
)

logger = Logger("append-missing-lakes", False)

mongo = MongoDriver(logger)

missing_lakes_data_folder = os.path.abspath("/Volumes/Files/Lakes data/missing-lakes")


def extract_info_from_file_name(file_name):
    parts = file_name.split(".")

    return {
        "fishnet": parts[2],
        "fish_id": parts[4],
        "hylak_id": parts[6],
        "buffer": parts[7].split("m")[0],
    }


def process_data_from_file(file_path):
    asset_data = extract_info_from_file_name(file_path)

    fishnet = int(asset_data["fishnet"])
    fish_id = int(asset_data["fish_id"])
    hylak_id = int(asset_data["hylak_id"])
    buffer = int(asset_data["buffer"])

    records = []
    try:
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            next(reader)  # skip header

            for observation in reader:
                if len(observation) != 0:
                    records.append(observation)

            if len(records) != 0:
                lakes_data_folder = (
                    f"/Volumes/lakes/Landsat 8/Fishnet {fishnet}/fish_ID{fish_id}"
                )

                for asset_file in os.listdir(os.path.abspath(lakes_data_folder)):
                    if re.compile(
                        get_asset_file_regex(
                            get_collection_from_fishnet(fishnet), buffer
                        )
                    ).match(asset_file):
                        asset_file_path = os.path.join(lakes_data_folder, asset_file)
                        print(asset_file_path)
                        with open(asset_file_path, "a") as asset_file:
                            writer = csv.writer(asset_file)
                            writer.writerows(records)
                            print(f"{len(records)} rows written")
            else:
                logger.log_info(
                    f"No records found for lake {hylak_id} in fish_id {fish_id}, fishnet {fishnet} "
                )
    except Exception as e:
        logger.log_error(f"Couldnt read file {file_path}: {e}")


if __name__ == "__main__":
    for idx in range(1, 12):
        folder = os.path.join(missing_lakes_data_folder, str(idx))
        missing_lakes_data = os.listdir(folder)

        for file in missing_lakes_data:
            file_path = os.path.join(folder, file)
            process_data_from_file(file_path)
