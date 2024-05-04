import concurrent.futures
import csv
from pprint import pprint

from Mongo.MongoAdapter import MongoDriver

DATASET = "L8"
ASSET_COLLECTION = 3


def get_assets_from_lookup_collection():
    assets = MongoDriver.find_all("lookup", "collection", ASSET_COLLECTION)
    if assets is None:
        return []
    else:
        return assets


def get_collection_count(collection_name):
    return MongoDriver.get_document_count(collection_name)


def get_dataset_name():
    if DATASET == "L8":
        return "Landsat8"
    else:
        return "Sentinel2"


def generate_collection_names_grouped_by_asset():
    assets = list(
        {
            int(doc["asset_id"])
            for doc in get_assets_from_lookup_collection()
            if "asset_id" in doc
        }
    )

    assets.sort()

    collection_names_per_asset = []

    for asset in assets:
        collection_names_per_asset.append(
            f"c{ASSET_COLLECTION}_{DATASET.lower()}_{asset}"
        )

    return collection_names_per_asset


def get_stats_for_asset(collection_name):
    existing_collections = MongoDriver.get_collection_names()

    print(existing_collections)

    properties = collection_name.split("_")

    asset_id = properties[2]

    asset_stats = [asset_id]

    if collection_name in existing_collections:
        asset_stats.append(str(get_collection_count(collection_name)))
    else:
        asset_stats.append("-")

    print("Stats for asset {}: ".format(asset_id), asset_stats)

    return asset_stats


def generate_report():
    collections = generate_collection_names_grouped_by_asset()

    report_file_name = "Reports/{} - Fishnet {} Report.csv".format(
        get_dataset_name(), ASSET_COLLECTION
    )

    with open(report_file_name, mode="w", newline="") as file:
        writer = csv.writer(file)
        # header row
        writer.writerow(["Asset", "Count"])

        with concurrent.futures.ProcessPoolExecutor(max_workers=50) as executor:
            asset_collections_stats = list(
                executor.map(get_stats_for_asset, collections)
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
