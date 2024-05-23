import os
import csv
from Parsing.Landsat8ParsingStrategy import Landsat8ParsingStrategy
from Services.Logger import Logger

logger = Logger(
    "export-data-from-raw-file-info.log", "export-data-from-raw-file-error.log", False
)

# missing_data_folders_path = "E:/missing-lakes/"
missing_data_folders_path = "/Volumes/Files/Lakes data/missing-lakes/"


def extract_info_from_file_name(file_name):
    parts = file_name.split(".")

    return {
        "fishnet": parts[2],
        "fish_id": parts[4],
        "hylak_id": parts[6],
        "buffer": parts[7].split("m")[0],
    }


def format_output_data(observation, hylak_id, fishnet, fish_id):
    return {
        "hylak_id": hylak_id,
        "fishnet": fishnet,
        "fish_id": fish_id,
        "image_sat": "Landsat8",
        "image_id": observation["image"]["id"],
        "image_date": observation["image"]["date"],
        "cloud_cover": observation["image"]["cloud_cover"],
        "sr_b1_60": observation["sr_band1"]["60"],
        "sr_b2_60": observation["sr_band2"]["60"],
        "sr_b3_60": observation["sr_band3"]["60"],
        "sr_b4_60": observation["sr_band4"]["60"],
        "sr_b5_60": observation["sr_band5"]["60"],
        "sr_b7_60": observation["sr_band7"]["60"],
        "qa_pixel_60": observation["qa_pixel"]["60"],
        "qa_radsat_60": observation["qa_radsat"]["60"],
        "sr_qa_aerosol_60": observation["sr_qa_aerosol"]["60"],
    }


def process_data_from_file(file_name, directory, writer):
    asset_data = extract_info_from_file_name(file_name)

    fishnet = int(asset_data["fishnet"])
    fish_id = int(asset_data["fish_id"])
    hylak_id = int(asset_data["hylak_id"])
    buffer = int(asset_data["buffer"])

    file_path = os.path.join(directory, file_name)

    parser = Landsat8ParsingStrategy()

    try:
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            next(reader)  # skip header

            records = []

            for observation in reader:
                if len(observation) != 0:
                    try:
                        image_record = parser.extract_image_record(observation)

                        record = parser.build_observation(observation, buffer)
                        record["image"] = image_record

                        if (
                            float(record["image"]["cloud_cover"]) <= 50
                            and int(float(fish_id)) < 1000
                        ):
                            records.append(
                                format_output_data(
                                    record, hylak_id, fishnet, fish_id
                                ).values()
                            )
                    except Exception as e:
                        logger.log_error(
                            f"Error parsing observation for asset {asset_data["fish_id"]}: {e}"
                        )
            else:
                logger.log_info(f"processed data for {file_name}")

            writer.writerows(records)

    except Exception as e:
        logger.log_error(f"Couldnt read file {file_name}: {e}")


if __name__ == "__main__":
    with open(os.path.abspath("landsat8-missing-lakes.csv"), mode="w") as output:
        export_writer = csv.writer(output)

        export_writer.writerow(
            [
                "hylak_id",
                "fishnet",
                "fish_id",
                "image_sat",
                "image_id",
                "image_date",
                "cloud_cover",
                "sr_b1_60",
                "sr_b2_60",
                "sr_b3_60",
                "sr_b4_60",
                "sr_b5_60",
                "sr_b7_60",
                "qa_pixel_60",
                "qa_radsat_60",
                "sr_qa_aerosol_60",
            ]
        )

        for section in list(range(1, 12)):
            section_folder = os.path.abspath(
                os.path.join(missing_data_folders_path, str(section))
            )
            missing_lakes_data_files = os.listdir(section_folder)

            for file in missing_lakes_data_files:
                process_data_from_file(file, section_folder, export_writer)

            print(f"{section_folder}")
