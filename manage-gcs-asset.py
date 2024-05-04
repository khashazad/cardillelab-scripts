from google.cloud import storage
import os
import csv
from Constants.Constants import (
    Collections,
    Operations,
    Datasets,
)
from Services.LoggerService import LoggerService as Logger

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    os.path.dirname(os.path.realpath(__file__)) + r"/key.json"
)


BUCKET = "landsat8-fishnet3-lake-harvest"
BUFFERS = [60]
Collection = Collections.Collection3
Operation = Operations.DOWNLOAD
Dataset = Datasets.LANDSAT8

PATH_ASSETS_TO_PROCESS = (
    os.path.dirname(os.path.realpath(__file__)) + r"/Assets/assets-to-process.csv"
)

DOWNLOAD_PATH = "/Volumes/Files/"


def getDatasetPrefix(dataset: Datasets):
    if dataset == Datasets.LANDSAT8:
        return "landsat8"
    elif dataset == Datasets.SENTINEL1:
        return "sentinel1"
    else:
        return "Unknown"


def getCollectionName(collection: Collections):
    if collection == Collections.Collection1:
        return "fishnet1"
    if collection == Collections.Collection2:
        return "fishnet2"
    if collection == Collections.Collection3:
        return "fishnet3"


###############################################
# Using the collectionId, assetId, asset_size and buffer,
# builds the string representing the file in CS
###############################################
def buildGCSFileName(asset_id, asset_size, buffer):
    dataset = getDatasetPrefix(Dataset)
    collection = getCollectionName(Collection)

    if Collection == Collections.Collection1 or Collection == Collections.Collection3:
        return "{0}.{1}.ID{2}.{3}.{4}m.csv".format(
            dataset, collection, asset_id, asset_size, buffer
        )
    else:
        return "{0}.{1}.fish.ID{2}.0.{3}.{4}m.csv".format(
            dataset, collection, asset_id, asset_size, buffer
        )


def buildDownloadFolderPath():
    dataset = getDatasetPrefix(Dataset)
    collection = getCollectionName(Collection)

    directory = DOWNLOAD_PATH + "{0} - {1}".format(
        dataset.capitalize(), collection.capitalize()
    )

    if not os.path.exists(directory):
        os.makedirs(directory)

    return directory


def buildStoredFileName(asset_id, asset_size, buffer):
    dataset = getDatasetPrefix(Dataset)
    return r"{0}.fishID{1}.{2}.{3}m.csv".format(dataset, asset_id, asset_size, buffer)


################################################
# Takes the asset_id and asset_size as parameter,
# downloads and organizes all the files for that
# asset from Cloud Storage into downloads folder
################################################
def downloadFiles(file_name, asset_id, asset_size, buffer):
    # Path of the folder containing files for the current fishID
    folderPath = os.path.join(buildDownloadFolderPath(), "fish_ID{}".format(asset_id))

    # if folder does not exist, create it
    if not os.path.exists(folderPath):
        os.makedirs(folderPath)

    # Initialise a client
    storage_client = storage.Client()
    # Create a bucket object for our bucket
    bucket = storage_client.bucket(BUCKET)
    # Create a blob object from the filepath
    blob = bucket.blob(file_name)

    # Download the file to a destination
    downloadFilePath = os.path.join(
        folderPath, buildStoredFileName(asset_id, asset_size, buffer)
    )

    blob.download_to_filename(downloadFilePath)


# Takes the fishID and last index as parameter,
# deletes all the files for that asset from Cloud Storage
##########################################################
def deleteFiles(file_name):
    # Initialise a client
    storage_client = storage.Client()
    # Create a bucket object for our bucket
    bucket = storage_client.bucket(BUCKET)
    # Create a blob object from the filepath
    blob = bucket.blob(file_name)
    # Delete the object from CS
    blob.delete()

    print("deleted: " + file_name)


def processAsset(asset_id, asset_size):
    for buffer in BUFFERS:
        # building the string representing the file in CS
        file_name = buildGCSFileName(asset_id, asset_size, buffer)

        try:
            if Operation == Operations.DOWNLOAD:
                print(f"download file {file_name}")
                downloadFiles(file_name, asset_id, asset_size, buffer)
        except Exception as e:
            print(e)
            Logger.log_error("Asset file not found: {}".format(asset_id))
        else:
            Logger.log_info("Processed file {}".format(file_name))

        if Operation == Operations.DELETE:
            deleteFiles(file_name)


#####################################################
# Reads the toProcess.csv files and for every entry,
# calls the correct method (download/delete) based on
# the user's choice
#####################################################
def processAssets():
    with open(PATH_ASSETS_TO_PROCESS) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")

        for row in csv_reader:
            if len(row) != 0:
                processAsset(row[0], row[1])


if __name__ == "__main__":
    processAssets()
