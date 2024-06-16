from Parsing.Landsat8ParsingStrategy import Landsat8ParsingStrategy
from Constants.Constants import Datasets, Collections


def get_record_parser(dataset):
    if dataset == Datasets.LANDSAT8:
        return Landsat8ParsingStrategy()
    else:
        return Landsat8ParsingStrategy()


def get_collection_id(collection):
    if collection == Collections.Collection1:
        return "1"
    if collection == Collections.Collection2:
        return "2"
    if collection == Collections.Collection3:
        return "3"
    return ""


def get_dataset_id(dataset):
    if dataset == Datasets.LANDSAT8:
        return "l8"
    if dataset == Datasets.SENTINEL1:
        return "s2"
    return ""


def get_assets_folder_dataset_prefix(dataset):
    if dataset == Datasets.LANDSAT8:
        return "Landsat 8"
    if dataset == Datasets.SENTINEL1:
        return "Sentinel 1"
    return ""


def get_assets_folder_path(collection, dataset):
    return "{} - Fishnet{}".format(
        get_assets_folder_dataset_prefix(dataset), get_collection_id(collection)
    )


def get_asset_file_regex(collection, buffer):
    if collection == Collections.Collection2:
        return r"[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.{0}m.csv".format(
            buffer
        )
    else:
        return r"[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.{0}m.csv".format(buffer)


def build_database_collection_name(collection, dataset, asset_id: str):
    return "c{}_{}_{}".format(
        get_collection_id(collection), get_dataset_id(dataset), asset_id
    )
