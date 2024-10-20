import csv
import multiprocessing
import os
import re

from Constants.Constants import Collections, Datasets
from Mongo.MongoAdapterV2 import MongoDriver
from Services.LoggerV2 import Logger
from utils.utils import (
    build_database_collection_name,
    get_asset_file_regex,
    get_assets_folder_path,
    get_collection_id,
    get_record_parser,
)

logger = Logger("insert-observations", format=False)
mongo = MongoDriver(logger, local=True)

# Configuration
COLLECTION = Collections.Collection3
DATASET = Datasets.LANDSAT8
BUFFERS = [60]

# Paths
PATH_DB_Assets_FOLDER = os.path.abspath("/Volumes/lakes/")

PATH_ASSETS_INSERT_DB = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), r"Assets/assets-to-insert.csv"
)


def get_observation_hash(observation):
    return "{}_{}".format(observation[2], observation[20])


def process_data(observation_records, file_path, buffer, asset_id):
    parsing_strategy = get_record_parser(DATASET)
    try:
        if not os.path.isfile(file_path):
            print(f"Not found: ${file_path}")
            mongo.partial_update_one(
                "asset_stats",
                {
                    "collection": int(get_collection_id(COLLECTION)),
                    "asset_id": int(asset_id),
                },
                {"raw_observations": 0},
            )
            return
        with open(file_path, "r") as file:
            reader_count = csv.reader(file)
            observation_count = len(list(reader_count)) - 1

            file.seek(0)
            reader = csv.reader(file)
            next(reader)  # skip header

            for observation in reader:
                if len(observation) != 0:
                    try:
                        observation_hash = get_observation_hash(observation)

                        if observation_hash in observation_records:
                            parsing_strategy.update_observation(
                                observation_records[observation_hash],
                                observation,
                                buffer,
                            )
                        else:
                            image_record = parsing_strategy.extract_image_record(
                                observation
                            )

                            if (
                                COLLECTION == Collections.Collection2
                                and int(asset_id) == 14
                            ):
                                date = image_record["date"].split("/")
                                image_record["date"] = f"{date[2]}-{date[0]}-{date[1]}"

                            record = parsing_strategy.build_observation(
                                observation, buffer
                            )
                            record["image"] = image_record

                            observation_records[observation_hash] = record
                    except Exception as e:
                        logger.log_error(
                            f"Error parsing observation for asset {asset_id}: {e}"
                        )

                mongo.partial_update_one(
                    "asset_stats",
                    {
                        "collection": int(get_collection_id(COLLECTION)),
                        "asset_id": int(asset_id),
                    },
                    {"raw_observations": int(observation_count)},
                )
    except Exception as e:
        logger.log_error(f"Couldnt read file {file_path}: {e}")


def get_asset_id_from_file_name(file_name):
    split_file_name = file_name.split(".")

    if COLLECTION == Collections.Collection1 or COLLECTION == Collections.Collection3:
        return split_file_name[1].split("D")[1]
    else:
        return split_file_name[2].split("D")[1]


# Parses the data for a single asset and inserts it to the database
def process_asset(asset_id):
    logger.log_info("Processing assest {}".format(asset_id))

    folder_path = os.path.join(
        PATH_DB_Assets_FOLDER,
        get_assets_folder_path(COLLECTION, DATASET),
        "fish_ID{}".format(asset_id),
    )

    print(folder_path)

    if not os.path.exists(folder_path):
        print(f"Not found: ${folder_path}")
        mongo.partial_update_one(
            "asset_stats",
            {
                "collection": int(get_collection_id(COLLECTION)),
                "asset_id": int(asset_id),
            },
            {
                "inserted_observations": 0,
                "exported_lakes": 0,
            },
        )
        return

    all_files = os.listdir(folder_path)

    observation_records = {}

    for buffer in BUFFERS:
        for file in all_files:
            s = get_asset_file_regex(COLLECTION, buffer)

            regex = re.compile(s)
            if regex.match(file):
                asset_id = get_asset_id_from_file_name(file)
                file_path = os.path.join(folder_path, file)

                process_data(observation_records, file_path, buffer, asset_id)

    record_collection_name = build_database_collection_name(
        COLLECTION, DATASET, asset_id
    )

    try:
        print("inserting records")
        inserted_count = mongo.insert_many(
            record_collection_name, observation_records.values()
        )

        mongo.partial_update_one(
            "asset_stats",
            {
                "collection": int(get_collection_id(COLLECTION)),
                "asset_id": int(asset_id),
            },
            {
                "inserted_observations": inserted_count,
                "exported_lakes": len(
                    list(mongo.db[record_collection_name].find({}).distinct("hylak_id"))
                ),
            },
        )

    except Exception as error:
        logger.log_error(error)


def process_assets():
    assets_to_insert = []
    with open(PATH_ASSETS_INSERT_DB) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for asset in csv_reader:
            if len(asset) != 0:
                asset_id = asset[0]

                assets_to_insert.append(asset_id)

                # process_asset(asset_id)

    num_processes = 10

    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(process_asset, assets_to_insert)


if __name__ == "__main__":
    process_assets()
