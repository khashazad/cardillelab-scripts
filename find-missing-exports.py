from google.cloud import storage
import os
from pprint import pprint
import csv

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    os.path.dirname(os.path.realpath(__file__)) + r"/key.json"
)


def get_all_exports(bucket_name):
    exports = set()
    # Initialize the GCS client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # List all objects in the bucket and download them
    blobs = bucket.list_blobs()

    for blob in blobs:
        parts = blob.name.split(".")

        exports.add((parts[2], parts[4], parts[6]))

    return exports


# Usage

part = 3

bucket_name = f"missing-lakes-data-{part}"
# bucket_name = "missing-lakes-data"

missing_lakes_path = os.path.abspath(
    f"/Users/khxsh/Desktop/missing-lakes/missing-lakes-{part}.csv"
)

missing_exports_output_path = os.path.abspath(
    f"/Users/khxsh/Desktop/missing-lakes/missing-exports/{part}.csv"
)
if __name__ == "__main__":
    exports = get_all_exports(bucket_name)

    with open(missing_lakes_path, mode="r") as file:
        # Create a CSV reader object
        csv_reader = csv.reader(file)

        # Read the header (first row)
        header = next(csv_reader)

        with open(missing_exports_output_path, mode="w") as output_file:
            csvwriter = csv.writer(output_file)

            csvwriter.writerow(header)

            found_missing_export = False

            # Read the rest of the rows
            for row in csv_reader:
                missing_lake = (row[4], row[3], row[0])
                if missing_lake not in exports:
                    csvwriter.writerow(row)
                    found_missing_export = True

            if found_missing_export:
                print("found missing export")
            else:
                print("all assets exported")
