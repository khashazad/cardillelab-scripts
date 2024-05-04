from Mongo.MongoAdapter import MongoDriver
import re


if __name__ == "__main__":
    collections = MongoDriver.get_collection_names()

    regex = re.compile(r"^c(1|2|3)_\w{2}_\d{1,3}_\d{1,3}m$")

    for collection in collections:
        if re.match(regex, collection):
            print("deleting collection: {}".format(collection))
            MongoDriver.drop_collection_if_exists(collection)
