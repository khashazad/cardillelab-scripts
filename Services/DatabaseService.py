import csv
import os
import re

from Constants.Constants import Datasets, Collections, PATH_AS
import Constants.Constants as constants

# from multiprocessing import Process
from Mongo.MongoAdapter import MongoDriver
from Parsing.Landsat8ParsingStrategy import Landsat8ParsingStrategy
from Services.LoggerService import LoggerService as Logger

from multiprocessing import Process


def process_data(dataset, filePath, asset_id, buffer):
    records = []

    record_collection_name = generate_collection_name(dataset, asset_id, buffer)

    parsing_strategy = get_record_parser(dataset)

    with open(filePath, "r") as file:
        Logger.log_info("Procesing file: " + filePath)

        reader = csv.reader(file)
        next(reader)  # skip header

        for observation in reader:
            image_record = parsing_strategy.extract_image_record(observation)
            record = parsing_strategy.build_observation(observation)
            record["image"] = image_record
            records.append(record)
        try:
            MongoDriver.insert_many_reset_collection(record_collection_name, records)
        except Exception as error:
            Logger.log_error(error)


def get_record_parser(dataset):
    if dataset == Datasets.LANDSAT8:
        return Landsat8ParsingStrategy()
    else:
        return Landsat8ParsingStrategy()


def generate_collection_name(dataset: Datasets, asset_id: str, buffer: str):
    collection_prefix = "l8" if dataset == Datasets.LANDSAT8 else "s2"

    return "c2_{}_{}_{}m".format(collection_prefix, asset_id, buffer)


def generate_image_collection_name(dataset: Datasets):
    collection_prefix = "l8" if dataset == Datasets.LANDSAT8 else "s2"

    return "{}_image_properties".format(collection_prefix)


def process_assets(dataset: Datasets):
    with open(constants.PATH_ASSETS_INSERT_DB) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for asset in csv_reader:
            if len(asset) != 0:
                asset_id = asset[0]

                process_asset(dataset, asset_id)


def getCollectionId(collection: Collections):
    if collection == Collections.Collection1:
        return "1"
    if collection == Collections.Collection2:
        return "2"
    if collection == Collections.Collection2:
        return "3"
    return ""


def getDatasetId(dataset: Datasets):
    if dataset == Datasets.LANDSAT8:
        return "Landsat8"
    if dataset == Datasets.SENTINEL2:
        return "Sentinel2"
    return ""


def getAssetsFolderName(collection: Collections, dataset: Datasets):
    return "{} - Fishnet {}".format(getDatasetId, getCollectionId(collection))


# Parses the data for a single asset and inserts it to the database
def process_asset(dataset, asset_id):
    Logger.log_info("Processing assest {}".format(asset_id))

    folder_path = "{}/{}/fish_ID{}".format(
        constants.PATH_DB_Assets_FOLDER,
        getAssetsFolderName(Collections.Collection2, dataset),
        asset_id,
    )

    all_files = os.listdir(folder_path)

    processes = []

    for buffer in constants.BUFFERS:
        for f in all_files:
            s = r"[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.{0}m.csv".format(
                buffer
            )
            regex = re.compile(s)
            if regex.match(f):
                split_file_name = f.split(".")
                asset_id = split_file_name[2].split("D")[1]
                file_path = os.path.join(folder_path, f)

                process = Process(
                    target=process_data, args=(dataset, file_path, asset_id, buffer)
                )

                processes.append(process)
                process.start()

                # process_data(dataset, file_path, asset_id, buffer)

    for p in processes:
        p.join()


def generate_lookup_collection():
    # list of lookup objects that will be inserted to the database collection
    lookup_records = []

    try:
        with open(constants.PATH_ASSETS_ALL) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")

            for asset in csv_reader:
                if len(asset) != 0:
                    asset_id = asset[0]

                    asset_file_path = (
                        constants.PATH_ASSETS_FISHNET2
                        + r"/fish_ID{}.csv".format(asset_id)
                    )

                    with open(asset_file_path, "r") as file:
                        reader = csv.reader(file)
                        next(reader)  # skip header

                        for row in reader:
                            lookup_records.append(
                                {
                                    "hylak_id": int(float(row[0])),
                                    "fish_id": int(float(asset_id)),
                                    "longitude": float(row[1]),
                                    "latitude": float(row[2]),
                                }
                            )

        MongoDriver.insert_many_reset_collection("fishnet_lookup", lookup_records)

        Logger.log_info("Inserted all lookup data")

    except Exception as error:
        Logger.log_error(error)
