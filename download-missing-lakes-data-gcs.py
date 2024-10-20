from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    os.path.dirname(os.path.realpath(__file__)) + r"/key.json"
)


def download_all_files(bucket_name, destination_folder):
    """Download all files from the specified GCS bucket to the destination folder."""
    # Initialize the GCS client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # List all objects in the bucket and download them
    blobs = bucket.list_blobs()

    count = 0
    for blob in blobs:
        blob.download_to_filename(os.path.join(destination_folder, blob.name))
        print(f"Downloaded {blob.name} to {destination_folder}")
        count += 1

    print(f"Downloaded {count} files")


# Usage

if __name__ == "__main__":
    for i in range(1, 12):
        if i == 1:
            bucket_name = "missing-lakes-data"
            destination_folder = f"/Volumes/Files/Lakes data/missing-lakes/{i}"
        else:
            bucket_name = f"missing-lakes-data-{i}"
            destination_folder = f"/Volumes/Files/Lakes data/missing-lakes/{i}"

        download_all_files(bucket_name, destination_folder)
