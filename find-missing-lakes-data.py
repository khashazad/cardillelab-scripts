import csv
import os
from Services.Logger import Logger
from pprint import pprint

import ee

ee.Authenticate()

PROJECT = "lakeharvest2021"

ee.Initialize(project=PROJECT)


logger = Logger("missing_lakes_info.log", "missing_lakes_error.log", False)

# variables
sensorScale = 30

buffer = 60

L8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")


def check_data_exists(hylak_id, fish_id, fishnet):
    print(f"lake: {hylak_id}, fish ID: {fish_id}, fishnet: {fishnet}")
    assetId = "fish_ID{0}".format(fish_id)

    # getting centroids and images
    centroids = ee.FeatureCollection(
        f"projects/api-project-269347469410/assets/LakeHarvest/Fishnets/Fishnet{fishnet}/{assetId}"
    )

    lake = ee.Feature(
        ee.FeatureCollection(
            centroids.filter(ee.Filter.eq("Hylak_id", ee.Number(int(hylak_id))))
        ).first()
    )

    image_count = (
        ee.ImageCollection(L8.filterBounds(ee.FeatureCollection(lake))).size().getInfo()
    )

    if image_count == 0:
        logger.log_info(
            f"No Data available for lake {hylak_id} in fish ID {fish_id}, fishnet {fishnet}"
        )
    else:
        logger.log_error(f"{fishnet}, {fish_id}, {hylak_id}")


if __name__ == "__main__":
    missing_lakes_file_path = os.path.abspath(
        "/Users/khxsh/Downloads/missing_50cloudcover.csv"
    )

    # missing_lakes_file_path = os.path.abspath("./test.csv")

    # Open the CSV file
    with open(missing_lakes_file_path, mode="r") as file:
        # Create a CSV reader object
        csv_reader = csv.reader(file)

        # Read the header (first row)
        header = next(csv_reader)
        print(f"Header: {header}")

        # Read the rest of the rows
        for row in csv_reader:
            check_data_exists(row[0], row[3], row[4])
