from posix import listdir
import pandas as pd
import os

EXPORT_FILES_DIRECTORY = os.path.abspath("E:/landsat8-exports")


def count_unique_lakes_in_export_file(file_path):
    data = []

    df = pd.read_csv(file_path)
    data.extend(df["hylak_id"].tolist())

    return len(list(set(data)))


if __name__ == "__main__":
    files = os.listdir(EXPORT_FILES_DIRECTORY)

    count = 0

    for file in files:
        file_path = os.path.join(EXPORT_FILES_DIRECTORY, file)

        lakes_in_export_file = count_unique_lakes_in_export_file(file_path)

        count += lakes_in_export_file

        print(f"{lakes_in_export_file} in {file}")

    print(f"Total lakes {count}")
