from Services.LoggerService import LoggerService as Logger
from pymongo import MongoClient


class MongoDriver:
    client = None
    tunnel = None

    db = None

    @staticmethod
    def get_db_instance():
        if MongoDriver.client is None:
            MongoDriver.initialize()

        return MongoDriver.db

    @staticmethod
    def initialize():
        if MongoDriver.client is None:
            # Connecting to MongoDB
            MongoDriver.client = MongoClient(
                host="206.12.90.121",
                port=27017,
                username="root",
                password="ORJ0Gcqo9cu0iG8Py6B2IYdZFBCyl7tQx4Iazr/VC6sYhrZIuXbvSkbM4J6Th0QO",
            )

            MongoDriver.db = MongoDriver.client["Lakes"]

    @staticmethod
    def close_connection():
        if MongoDriver.client is not None:
            MongoDriver.client.close()

    @staticmethod
    def drop_collection_if_exists(collectionName):
        db = MongoDriver.get_db_instance()
        if db is not None:
            db[collectionName].drop()

    @staticmethod
    def get_collection_names():
        db = MongoDriver.get_db_instance()
        if db is not None:
            return db.list_collection_names()
        else:
            return []

    @staticmethod
    def get_document_count(collection_name):
        db = MongoDriver.get_db_instance()
        if db is not None:
            collection = db[collection_name]
            return collection.count_documents({})

    @staticmethod
    def find_one(collection_name, field, value):
        db = MongoDriver.get_db_instance()

        if db is not None:
            collection = db[collection_name]
            if collection is not None:
                return collection.find_one({field: value})
            else:
                return None

    @staticmethod
    def insert_one(collection_name, document):
        db = MongoDriver.get_db_instance()

        if db is not None:
            collection = db[collection_name]
            try:
                insert_result = collection.insert_one(document)
                return insert_result.inserted_id
            except TypeError:
                Logger.log_error(
                    "TypeError occured when inserting documents to collection."
                )

    @staticmethod
    def find_all(collection_name, filter_key, filter_value):
        db = MongoDriver.get_db_instance()

        if db is not None:
            return db[collection_name].find({filter_key: filter_value})

    @staticmethod
    def insert_many_reset_collection(collection_name, documents):
        db = MongoDriver.get_db_instance()

        MongoDriver.drop_collection_if_exists(collection_name)

        Logger.log_info("Start batch insert to collection: " + collection_name)

        if db is not None:
            collection = db[collection_name]
            if collection is not None:
                try:
                    collection.insert_many(documents)
                except TypeError:
                    Logger.log_error(
                        "TypeError occured when inserting documents to collection."
                    )

            Logger.log_info("Completed batch insert to collection:  " + collection_name)

    @staticmethod
    def insert_many(collection_name, documents):
        db = MongoDriver.get_db_instance()

        if db is not None:
            MongoDriver.drop_collection_if_exists(collection_name)

            collection = db[collection_name]
            if collection is not None:
                try:
                    collection.insert_many(documents)
                except TypeError:
                    Logger.log_error(
                        "TypeError occured when inserting documents to collection."
                    )

    @staticmethod
    def find(collection_name, filter):
        db = MongoDriver.get_db_instance()

        if db is not None:
            collection = db[collection_name]
            documents = collection.find(filter)

            if documents is None:
                return []
            else:
                return list(documents)

    @staticmethod
    def aggregate(collection_name, pipeline):
        db = MongoDriver.get_db_instance()

        if db is not None:
            collection = db[collection_name]
            documents = collection.aggregate(pipeline)

            if documents is None:
                return []
            else:
                return list(documents)
        else:
            return []

    @staticmethod
    def remove(collection_name, filter):
        db = MongoDriver.get_db_instance()

        if db is not None:
            db[collection_name].delete_many(filter)
