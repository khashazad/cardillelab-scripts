import csv
import os

files = [
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID758/landsat8.fishID758.9441.60m.csv",
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID952/landsat8.fishID952.7793.60m.csv",
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID922/landsat8.fishID922.9963.60m.csv",
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID926/landsat8.fishID926.6830.60m.csv",
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID933/landsat8.fishID933.8982.60m.csv",
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID934/landsat8.fishID934.12188.60m.csv",
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID957/landsat8.fishID957.7745.60m.csv",
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID959/landsat8.fishID959.9499.60m.csv",
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID967/landsat8.fishID967.7887.60m.csv",
    "/Volumes/Files/Landsat8 - Fishnet 1/fish_ID984/landsat8.fishID984.7774.60m.csv",
]


def backup_file_name(file_path):
    parts = file_path.split(".csv")

    return f"{parts[0]}-old.csv"


def remove_duplicate_headers(file_path):
    if file_path.endswith(".csv"):
        old_file = backup_file_name(file_path)
        os.rename(file_path, backup_file_name(file_path))

        with open(old_file, "r", newline="") as infile, open(
            file_path, "w", newline=""
        ) as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # Read the first row as header and write it to the output file
            header = next(reader)
            writer.writerow(header)

            # Convert the header to a set for faster lookup
            header_set = set(header)

            # Write remaining rows that are not duplicates of the header
            for row in reader:
                if not set(row).issubset(header_set):
                    writer.writerow(row)
                else:
                    print(f"{row}")
                    print(f"header removed from {old_file}")


# Example usage:

if __name__ == "__main__":
    for file in files:
        remove_duplicate_headers(file)
