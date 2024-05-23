from Services.Logger import Logger
from Mongo.MongoAdapterV2 import MongoDriver
import os
import csv
from Parsing.Landsat8ParsingStrategy import Landsat8ParsingStrategy

logger = Logger(
    "insert-missing-fish-ids-info.log", "insert-missing-fish-ids-errors.log", False
)

mongo = MongoDriver(logger)


fishnet = 1
buffer = 60


def extract_info_from_file_name(file_name):
    parts = file_name.split(".")

    return {
        "fishnet": 1,
        "fish_id": parts[1].split("D")[1],
        "buffer": 60,
    }


def process_data_from_file(directory, file_name):
    asset_data = extract_info_from_file_name(file_name)

    fishnet = int(asset_data["fishnet"])
    fish_id = int(asset_data["fish_id"])
    buffer = int(asset_data["buffer"])

    file_path = os.path.join(directory, file_name)

    parser = Landsat8ParsingStrategy()

    try:
        with open(file_path) as file:
            reader = csv.reader(file)
            next(reader)  # skip header

            records = []

            for observation in reader:
                if len(observation) != 0:
                    try:
                        image_record = parser.extract_image_record(observation)

                        record = parser.build_observation(observation, buffer)
                        record["image"] = image_record

                        records.append(record)
                    except Exception as e:
                        logger.log_error(
                            f"Error parsing observation for asset {asset_data["fish_id"]}: {e}"
                        )
            else:
                collection = f"c{fishnet}_l8_{fish_id}"

                mongo.insert_many(collection, records)

    except Exception as e:
        logger.log_error(f"Couldnt read file {file_name}: {e}")


if __name__ == "__main__":
    with open("./Assets/missing-fish-ids-from-db.csv") as file:
        missing_fish_ids = csv.reader(file)

        for asset in missing_fish_ids:
            fishnet = asset[0]
            fish_id = asset[1]

            missing_fish_id_data_folder = os.path.abspath(
                f"/Volumes/Files/Lakes data/Landsat8 - Fishnet 1/fish_ID{fish_id}"
            )

            missing_lakes_data_files = os.listdir(missing_fish_id_data_folder)

            for file in missing_lakes_data_files:
                process_data_from_file(missing_fish_id_data_folder, file)
                print(f"processed {file}")
