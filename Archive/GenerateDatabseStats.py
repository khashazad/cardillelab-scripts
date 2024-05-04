import concurrent.futures
import csv
from collections import defaultdict
import re
from pprint import pprint

from Mongo.MongoAdapter import MongoDriver

BUFFERS = [1, 10, 15, 20, 30, 40, 60, 90, 100, 125, 250]
DATASET = "L8"


def get_collection_count(collection_name):
    return MongoDriver.get_document_count(collection_name)


def filter_collections_by_dataset(collections):
    regex = re.compile(r"^c\d{1}_l8_\d+_\d+m$")
    return [c for c in collections if regex.match(c)]


def group_by_asset_id(collections):
    groups = defaultdict(list)
    for col in collections:
        parts = col.split("_")

        asset_id = str(parts[2])
        asset_collection_id = str(parts[0])

        group_key = "{}_{}".format(asset_collection_id, asset_id)
        groups[group_key].append(col)

    return list(groups.values())


def get_dataset_name():
    if DATASET == "L8":
        return "Landsat8"
    else:
        return "Sentinel2"


def get_stats_for_asset(asset_collections):
    properties = asset_collections[0].split("_")

    asset_collection_id = properties[0][1:]
    dataset = properties[1]
    asset_id = properties[2]

    asset_stats = [asset_collection_id, asset_id]

    for buffer in BUFFERS:
        collection_name = "c{}_{}_{}_{}m".format(
            asset_collection_id, dataset, asset_id, buffer
        )

        if collection_name in asset_collections:
            asset_stats.append(str(get_collection_count(collection_name)))
        else:
            asset_stats.append("-")

    print("Stats for asset {}: ".format(asset_id), asset_stats)

    return asset_stats


def generate_report():
    collections = filter_collections_by_dataset(MongoDriver.get_collection_names())

    if collections is not None:
        collections_grouped_by_asset = group_by_asset_id(collections)

        with open(
            "{}_collections_report.csv".format(get_dataset_name()), mode="w", newline=""
        ) as file:
            writer = csv.writer(file)
            # header row
            writer.writerow(
                ["Collection", "Asset"] + [str(buffer) + "m" for buffer in BUFFERS]
            )

            with concurrent.futures.ProcessPoolExecutor(max_workers=50) as executor:
                asset_collections_stats = list(
                    executor.map(get_stats_for_asset, collections_grouped_by_asset)
                )

            # asset_collections_stats = []
            #
            # for asset_collections in collections_grouped_by_asset:
            #     asset_collections_stats.append(get_stats_for_asset(asset_collections))
            #

            writer.writerows(asset_collections_stats)

        print("CSV report generated successfully.")


if __name__ == "__main__":
    generate_report()
