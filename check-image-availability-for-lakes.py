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


def check_data_exists(lake_id, fish_id, fishnet):
    assetId = "fish_ID{0}".format(fish_id)

    # getting centroids and images
    centroids = ee.FeatureCollection(
        f"projects/api-project-269347469410/assets/LakeHarvest/Fishnets/Fishnet{fishnet}/{assetId}"
    )

    centroids = ee.FeatureCollection(
        centroids.filter(ee.Filter.eq("Hylak_id", lake_id))
    )

    # Subcollection of centroids
    if centroids.size().getInfo() == 0:
        logger.log_info(
            f"No Data available for lake {lake_id} in fish ID {fish_id}, fishnet {fishnet}"
        )
        return
    centroids = centroids.toList(centroids.size()).getInfo()

    def processImages(img):
        img = ee.Image(
            img.select(
                [
                    "SR_B1",
                    "SR_B2",
                    "SR_B3",
                    "SR_B4",
                    "SR_B5",
                    "SR_B6",
                    "SR_B7",
                    "ST_B10",
                    "SR_QA_AEROSOL",
                    "QA_PIXEL",
                    "QA_RADSAT",
                ]
            )
            .reduceNeighborhood(
                **{
                    "reducer": ee.Reducer.mean(),
                    "kernel": ee.Kernel.square(buffer, "meters"),
                }
            )
            .rename(
                [
                    "SR_B1_{0}".format(buffer),
                    "SR_B2_{0}".format(buffer),
                    "SR_B3_{0}".format(buffer),
                    "SR_B4_{0}".format(buffer),
                    "SR_B5_{0}".format(buffer),
                    "SR_B6_{0}".format(buffer),
                    "SR_B7_{0}".format(buffer),
                    "ST_B10_{0}".format(buffer),
                    "SR_QA_AEROSOL_{0}".format(buffer),
                    "QA_PIXEL_{0}".format(buffer),
                    "QA_RADSAT_{0}".format(buffer),
                ]
            )
            .set("system:time_start", img.get("system:time_start"))
            .copyProperties(img)
            .set("imgID", img.get("system:id"))
        )

        # Get metadata for the image.
        imgDate = img.date().format("YYYY-MM-dd")
        imgDateYear = img.date().format("YYYY")
        imgDateMonth = img.date().format("MM")
        imgDateDay = img.date().format("dd")
        imgDateDOY = img.date().format("DDD")

        cloudCover = img.get("CLOUD_COVER")
        cloudCoverLand = img.get("CLOUD_COVER_LAND")

        imgId = img.getString("imgID")
        imgSat = "LANDSAT8"

        # Filter the points to those that intersect the image.

        bandStats = img.sampleRegions(centroids).map(
            lambda pt: pt.set(
                {
                    "imgDate": imgDate,
                    "imgYear": imgDateYear,
                    "imgMonth": imgDateMonth,
                    "imgDay": imgDateDay,
                    "imgDOY": imgDateDOY,
                    "cloudCover": cloudCover,
                    "cloudCoverLand": cloudCoverLand,
                    "imgId": imgId,
                    "BufferSize": buffer,
                    "imgSat": imgSat,
                }
            )
        )

        return bandStats

    LC08col = (
        L8.filterBounds(centroids)
        .map(processImages)
        .flatten()
        .filter(
            ee.Filter.notNull(
                [
                    "SR_B1_{0}".format(buffer),
                    "SR_B2_{0}".format(buffer),
                    "SR_B3_{0}".format(buffer),
                    "SR_B4_{0}".format(buffer),
                    "SR_B5_{0}".format(buffer),
                    "SR_B6_{0}".format(buffer),
                    "SR_B7_{0}".format(buffer),
                    "ST_B10_{0}".format(buffer),
                    "SR_QA_AEROSOL_{0}".format(buffer),
                    "QA_PIXEL_{0}".format(buffer),
                    "QA_RADSAT_{0}".format(buffer),
                ]
            )
        )
    )

    image_count = int(LC08col.size().getInfo())

    if image_count == 0:
        logger.log_info(
            f"No Data available for lake {lake_id} in fish ID {fish_id}, fishnet {fishnet}"
        )
    else:
        logger.log_error(f"{fishnet}, {fish_id}, {lake_id}")


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
