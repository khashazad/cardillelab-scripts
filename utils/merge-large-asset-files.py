import os
import re

ASSETS = [49]

SPLIT_FILE_PATH = os.path.abspath("/Volumes/Files/fishID49/")

OUTPUT_PATH = os.path.abspath("/Volumes/Files/Landsat8 - Fishnet3")

total_lake_count = 1063


def get_files_matching_regex(directory, regex_pattern):
    matched_files = []
    regex = re.compile(regex_pattern)
    for filename in os.listdir(directory):
        if regex.match(filename):
            matched_files.append(os.path.join(directory, filename))
    return matched_files


def merge_files(files, output_file_path):
    is_first_header = True

    for file in files:
        with open(file, "r") as reader:
            if is_first_header:
                is_first_header = False
            else:
                next(reader)

            with open(output_file_path, "a") as output:
                for line in reader:
                    output.write(line)


def merge_asset_files(asset):
    output_folder = os.path.join(OUTPUT_PATH, f"fish_ID{asset}")

    input_files = get_files_matching_regex(SPLIT_FILE_PATH, f"^.*ID{asset}")

    input_files.sort()

    # totla_lake_count = 0
    # for file in input_files:
    #     totla_lake_count += int(file.split(".")[3])

    for f in os.listdir(output_folder):
        os.unlink(os.path.join(output_folder, f))

    output_file_path = os.path.join(
        output_folder, f"landsat8.fishID{asset}.{total_lake_count}.60m.csv"
    )

    print(output_file_path)

    merge_files(input_files, output_file_path)


if __name__ == "__main__":
    for asset in ASSETS:
        merge_asset_files(asset)
