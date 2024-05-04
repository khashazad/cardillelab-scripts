from Constants.Constants import Datasets, Collections

from Mongo.MongoAdapter import MongoDriver
from Parsing.Landsat8ParsingStrategy import Landsat8ParsingStrategy
from Services.LoggerService import LoggerService as Logger

import csv
import os
import re

# Configuration
COLLECTION = Collections.Collection3
DATASET = Datasets.LANDSAT8
BUFFERS = [60]

# Paths
PATH_DB_Assets_FOLDER = os.path.abspath("/Volumes/Files/")

PATH_ASSETS_INSERT_DB = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), r"Assets/assets-to-insert.csv"
)


def get_record_parser():
    if DATASET == Datasets.LANDSAT8:
        return Landsat8ParsingStrategy()
    else:
        return Landsat8ParsingStrategy()


def generate_collection_name(asset_id: str):
    return "c{}_{}_{}".format(get_collection_id(), get_dataset_id(), asset_id)


def get_collection_id():
    if COLLECTION == Collections.Collection1:
        return "1"
    if COLLECTION == Collections.Collection2:
        return "2"
    if COLLECTION == Collections.Collection3:
        return "3"
    return ""


def get_dataset_id():
    if DATASET == Datasets.LANDSAT8:
        return "l8"
    if DATASET == Datasets.SENTINEL1:
        return "s2"
    return ""


def get_assets_folder_dataset_prefix():
    if DATASET == Datasets.LANDSAT8:
        return "Landsat8"
    if DATASET == Datasets.SENTINEL1:
        return "Sentinel1"
    return ""


def get_assets_folder_path():
    return "{} - Fishnet{}".format(
        get_assets_folder_dataset_prefix(), get_collection_id()
    )


def get_asset_file_regex(buffer):
    if COLLECTION == Collections.Collection2:
        return r"[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.{0}m.csv".format(
            buffer
        )
    else:
        return r"[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.{0}m.csv".format(buffer)


def get_observation_hash(observation):
    return "{}_{}".format(observation[2], observation[20])


def process_data(observation_records, filePath, buffer, asset_id):
    parsing_strategy = get_record_parser()

    try:
        with open(filePath, "r") as file:
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
                        Logger.log_error(
                            f"Error parsing observation for asset {asset_id}: {e}"
                        )
    except Exception as e:
        Logger.log_error(f"Couldnt read file {filePath}: {e}")


def get_asset_id_from_file_name(file_name):
    split_file_name = file_name.split(".")

    if COLLECTION == Collections.Collection1 or COLLECTION == Collections.Collection3:
        return split_file_name[1].split("D")[1]
    else:
        return split_file_name[2].split("D")[1]


# Parses the data for a single asset and inserts it to the database
def process_asset(asset_id):
    Logger.log_info("Processing assest {}".format(asset_id))

    folder_path = os.path.join(
        PATH_DB_Assets_FOLDER, get_assets_folder_path(), "fish_ID{}".format(asset_id)
    )

    all_files = os.listdir(folder_path)

    processes = []

    observation_records = {}

    for buffer in BUFFERS:
        for file in all_files:
            s = get_asset_file_regex(buffer)

            regex = re.compile(s)
            if regex.match(file):
                asset_id = get_asset_id_from_file_name(file)
                file_path = os.path.join(folder_path, file)

                # process = Process(
                #     target=process_data, args=(file_path, asset_id, buffer)
                # )
                #
                # processes.append(process)

                process_data(observation_records, file_path, buffer, asset_id)

    record_collection_name = generate_collection_name(asset_id)

    try:
        MongoDriver.insert_many_reset_collection(
            record_collection_name, observation_records.values()
        )
    except Exception as error:
        Logger.log_error(error)

    # for p in processes:
    #     p.join()


def process_assets():
    with open(PATH_ASSETS_INSERT_DB) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for asset in csv_reader:
            if len(asset) != 0:
                asset_id = asset[0]

                process_asset(asset_id)


if __name__ == "__main__":
    process_assets()
