from Constants.Constants import Collections
from Mongo.MongoAdapter import MongoDriver
from Services.Logger import Logger
from Services.StaticCounter import StaticCounter as Counter

import os
import csv
import re

counter = Counter()
logger = Logger("export_info.log", "export_error.log")

COLLECTION = Collections.Collection2
CLOUD_THRESHOLD = 50.0
BUFFER = 60
# OUTPUT_FILE_PATH = os.path.join(
#     os.path.dirname(os.path.realpath(__file__)), r"Export/Landsat8-Fishnet2.csv"
# )
OUTPUT_FILE_PATH = os.path.abspath("/Volumes/Files/Lansat8-Fishnet1.csv")

COLLECTION_REGEX = re.compile(r"^c2_l8_\d{1,4}$")
# COLLECTION_REGEX = re.compile(r"^c2_l8_14$")

HEADERS = [
    "hylak_id",
    "fishnet",
    "fish_id",
    "image_sat",
    "image_id",
    "image_date",
    "cloud_cover",
    "sr_b1_{}".format(BUFFER),
    "sr_b2_{}".format(BUFFER),
    "sr_b3_{}".format(BUFFER),
    "sr_b4_{}".format(BUFFER),
    "sr_b5_{}".format(BUFFER),
    "sr_b7_{}".format(BUFFER),
    "qa_pixel_{}".format(BUFFER),
    "qa_radsat_{}".format(BUFFER),
    "sr_qa_aerosol_{}".format(BUFFER),
]


def get_collection_id():
    if COLLECTION == Collections.Collection1:
        return "1"
    if COLLECTION == Collections.Collection2:
        return "2"
    if COLLECTION == Collections.Collection2:
        return "3"
    return ""


def format_observation(obs):
    return [
        int(obs["hylak_id"]),
        int(obs["fishnet"]),
        int(obs["fish_id"]),
        obs["image_sat"],
        obs["image_id"],
        obs["image_date"],
        obs["cloud_cover"],
        obs["sr_b1_{}".format(BUFFER)],
        obs["sr_b2_{}".format(BUFFER)],
        obs["sr_b3_{}".format(BUFFER)],
        obs["sr_b4_{}".format(BUFFER)],
        obs["sr_b5_{}".format(BUFFER)],
        obs["sr_b7_{}".format(BUFFER)],
        obs["qa_pixel_{}".format(BUFFER)],
        obs["qa_radsat_{}".format(BUFFER)],
        obs["sr_qa_aerosol_{}".format(BUFFER)],
    ]


def write_rows_to_output_file(rows):
    with open(OUTPUT_FILE_PATH, "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(rows)


def export_collection_observations(collection_name):
    asset_id = collection_name.split("_")[2]
    pipeline = [
        {"$match": {"image.cloud_cover": {"$lt": CLOUD_THRESHOLD}}},
        {
            "$project": {
                "_id": 0,
                "hylak_id": 1,
                "fishnet": get_collection_id(),
                "fish_id": asset_id,
                "image_sat": "Landsat8",
                "image_id": "$image.id",
                "image_date": "$image.date",
                "cloud_cover": "$image.cloud_cover",
                "sr_b1_{}".format(BUFFER): "$sr_band1.{}".format(BUFFER),
                "sr_b2_{}".format(BUFFER): "$sr_band2.{}".format(BUFFER),
                "sr_b3_{}".format(BUFFER): "$sr_band3.{}".format(BUFFER),
                "sr_b4_{}".format(BUFFER): "$sr_band4.{}".format(BUFFER),
                "sr_b5_{}".format(BUFFER): "$sr_band5.{}".format(BUFFER),
                "sr_b7_{}".format(BUFFER): "$sr_band7.{}".format(BUFFER),
                "qa_pixel_{}".format(BUFFER): "$qa_pixel.{}".format(BUFFER),
                "qa_radsat_{}".format(BUFFER): "$qa_radsat.{}".format(BUFFER),
                "sr_qa_aerosol_{}".format(BUFFER): "$sr_qa_aerosol.{}".format(BUFFER),
            }
        },
    ]

    observations = MongoDriver.aggregate(collection_name, pipeline)

    sorted_observations = list(map((lambda obs: format_observation(obs)), observations))

    observation_count = len(sorted_observations)

    logger.log_info(
        "Writing {} records from collection {}".format(
            observation_count, collection_name
        )
    )
    write_rows_to_output_file(sorted_observations)
    logger.log_info("Finished writing records.")
    counter.increment(observation_count)


def process_collections():
    all_collections = MongoDriver.get_collection_names()

    for collection in all_collections:
        if COLLECTION_REGEX.match(collection):
            try:
                export_collection_observations(collection)
            except Exception as error:
                logger.log_error(
                    "Exception occured while fetching data for collection: {} -{}".format(
                        collection, error
                    )
                )


if __name__ == "__main__":
    # write the header
    write_rows_to_output_file([HEADERS])

    process_collections()
