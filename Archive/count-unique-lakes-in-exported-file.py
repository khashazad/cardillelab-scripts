import pandas as pd
import os

EXPORT_FILES_DIRECTORY = os.path.abspath("E:/landsat8-exports")


def unique_lakes_in_export_file(file_path):
    data = []

    df = pd.read_csv(file_path)
    data.extend(df["hylak_id"].tolist())

    return set(data)


if __name__ == "__main__":
    files = os.listdir(EXPORT_FILES_DIRECTORY)

    unique_lakes = set()

    for file in files:
        file_path = os.path.join(EXPORT_FILES_DIRECTORY, file)

        lakes_in_export_file = unique_lakes_in_export_file(file_path)

        unique_lakes.update(lakes_in_export_file)

        print(f"{file}: {len(unique_lakes)}")

    print(f"Total lakes {len(unique_lakes)}")
