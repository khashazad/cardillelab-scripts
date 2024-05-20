from Services.Logger import Logger
from Mongo.MongoAdapterV2 import MongoDriver
import os
import csv
from Parsing.Landsat8ParsingStrategy import Landsat8ParsingStrategy

logger = Logger(
    "insert-missing-lakes-info.log", "insert-missing-lakes-errors.log", False
)

mongo = MongoDriver(logger)

missing_lakes_data_folder = os.path.abspath("E:/missing-lakes/4")


def extract_info_from_file_name(file_name):
    parts = file_name.split(".")

    return {
        "fishnet": parts[2],
        "fish_id": parts[4],
        "hylak_id": parts[6],
        "buffer": parts[7].split("m")[0],
    }


def process_data_from_file(file_name):
    asset_data = extract_info_from_file_name(file_name)

    fishnet = int(asset_data["fishnet"])
    fish_id = int(asset_data["fish_id"])
    hylak_id = int(asset_data["hylak_id"])
    buffer = int(asset_data["buffer"])

    file_path = os.path.join(missing_lakes_data_folder, file_name)

    parser = Landsat8ParsingStrategy()

    try:
        with open(os.path.abspath("./lakes_with_no_data.csv"), mode="a") as file:
            writer = csv.writer(file)
            with open(file_path, "r") as file:
                reader = csv.reader(file)
                next(reader)  # skip header

                records = []

                no_data_flag = True

                for observation in reader:
                    if len(observation) != 0:
                        no_data_flag = False
                        try:
                            image_record = parser.extract_image_record(observation)

                            record = parser.build_observation(observation, buffer)
                            record["image"] = image_record

                            records.append(record)
                        except Exception as e:
                            logger.log_error(
                                f"Error parsing observation for asset {asset_data["fish_id"]}: {e}"
                            )

                if no_data_flag:
                    writer.writerow([fishnet, fish_id, hylak_id])
                else:
                    collection = f"c{fishnet}_l8_{fish_id}"

                    mongo.remove(collection, {"hylak_id": {"$eq": hylak_id}})

                    mongo.insert_many_append(collection, records)
                    logger.log_info(f"Inserted data for {asset_data}")

    except Exception as e:
        logger.log_error(f"Couldnt read file {file_name}: {e}")


if __name__ == "__main__":
    missing_lakes_data_files = os.listdir(missing_lakes_data_folder)

    for file in missing_lakes_data_files:
        process_data_from_file(file)
