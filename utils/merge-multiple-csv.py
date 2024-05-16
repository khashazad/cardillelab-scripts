import os

FILES_TO_MERGE_PATH = os.path.abspath("/Volumes/Files/fishID49/")

OUTPUT_FILE_PATH = os.path.abspath("/Volumes/Files/Landsat8 - Fishnet3")


def get_directory_files():
    files = []
    for filename in os.listdir(FILES_TO_MERGE_PATH):
        files.append(os.path.join(FILES_TO_MERGE_PATH, filename))
    return files


def merge_files(files):
    is_first_header = True

    for file in files:
        with open(file, "r") as reader:
            if is_first_header:
                is_first_header = False
            else:
                next(reader)

            with open(OUTPUT_FILE_PATH, "a") as output:
                for line in reader:
                    output.write(line)


if __name__ == "__main__":
    merge_files(get_directory_files())
