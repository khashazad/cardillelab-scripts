from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import sys


class MongoDriver:
    def __init__(
        self,
        logger=None,
        local=False,
    ):
        try:
            self.client = MongoClient(
                host="206.12.90.121",
                port=27017,
                username="root",
                password="ORJ0Gcqo9cu0iG8Py6B2IYdZFBCyl7tQx4Iazr/VC6sYhrZIuXbvSkbM4J6Th0QO",
            )
            if local:
                self.client = MongoClient(
                    host="127.0.0.1",
                    port=27017,
                    username="root",
                    password="ORJ0Gcqo9cu0iG8Py6B2IYdZFBCyl7tQx4Iazr/VC6sYhrZIuXbvSkbM4J6Th0QO",
                )
                print("Local database...")

            self.db = self.client["Lakes"]
            print("Connected to MongoDB")
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            print(f"Connection failed: {e}")
            sys.exit(1)

        self.logger = logger

    def close_connection(self):
        if self.client:
            self.client.close()

    def drop_collection_if_exists(self, collectionName):
        if self.db is not None:
            self.db[collectionName].drop()

    def get_collection_names(self):
        if self.db is not None:
            return self.db.list_collection_names()

    def get_document_count(self, collection_name):
        if self.db is not None:
            collection = self.db[collection_name]
            return collection.count_documents({})

    def find_one(self, collection_name, field, value):
        if self.db is not None:
            collection = self.db[collection_name]

            if collection is not None:
                return collection.find_one({field: value})
            else:
                return None

    def insert_one(self, collection_name, document):
        if self.db is not None:
            collection = self.db[collection_name]
            try:
                insert_result = collection.insert_one(document)
                self.log_info(
                    f"Inserted {len(insert_result.inserted_id)} records from collection {collection_name}"
                )
            except TypeError as e:
                self.log_error(
                    f"TypeError occured when inserting document to collection {collection_name}: {e}"
                )

    def find_all(self, collection_name, filter_key, filter_value):
        if self.db is not None:
            return self.db[collection_name].find({filter_key: filter_value})

    def insert_many(self, collection_name, documents):
        self.drop_collection_if_exists(collection_name)

        self.log_info(f"Start batch insert to collection: {collection_name}")

        if self.db is not None:
            collection = self.db[collection_name]
            if collection is not None:
                try:
                    insert_result = collection.insert_many(documents)
                    self.log_info(
                        f"Inserted {len(insert_result.inserted_ids)} records to collection {collection_name}"
                    )
                except TypeError as e:
                    self.log_error(
                        f"TypeError occured when inserting documents to collection {collection_name}: {e}"
                    )

            self.log_info("Completed batch insert to collection:  " + collection_name)

    def insert_many_append(self, collection_name, documents):
        if self.db is not None:
            self.drop_collection_if_exists(collection_name)

            collection = self.db[collection_name]
            if collection is not None:
                try:
                    insert_result = collection.insert_many(documents)
                    self.log_info(
                        f"Inserted {len(insert_result.inserted_ids)} records to collection {collection_name}"
                    )
                except TypeError as e:
                    self.log_error(
                        f"TypeError occured when inserting documents to collection {collection_name}: {e}"
                    )

    def find(self, collection_name, filter):
        if self.db is not None:
            collection = self.db[collection_name]
            documents = collection.find(filter)

            if documents is None:
                return []
            else:
                return list(documents)

    def aggregate(self, collection_name, pipeline):
        if self.db is not None:
            collection = self.db[collection_name]
            documents = collection.aggregate(pipeline)

            if documents is None:
                return []
            else:
                return list(documents)
        else:
            return []

    def remove(self, collection_name, filter):
        if self.db is not None:
            d = self.db[collection_name].delete_many(filter)
            self.log_info(
                f"Deleted {d.deleted_count} from collection {collection_name}"
            )

    def log_info(self, message):
        if self.logger is not None:
            self.logger.log_info(message)
        else:
            print(message)

    def log_error(self, message):
        if self.logger is not None:
            self.logger.log_error(message)
        else:
            print(message)
