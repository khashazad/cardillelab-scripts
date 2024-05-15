import re
from Mongo.MongoAdapter import MongoDriver
from Services.Logger import Logger

COLLECTION_REGEX = re.compile(r"^c2_l8_\d{1,4}$")


filter = {"image.year": {"$gte": 2021}}

logger = Logger("delete_info.log", "delete_error.log")


def delete_records(collection):
    MongoDriver.remove(collection, filter)


def process_collections():
    all_collections = MongoDriver.get_collection_names()

    for collection in all_collections:
        if COLLECTION_REGEX.match(collection):
            try:
                delete_records(collection)
            except Exception as error:
                logger.log_error(
                    "Exception occured while fetching data for collection: {} -{}".format(
                        collection, error
                    )
                )


if __name__ == "__main__":
    delete_records("c2_l8_14")
