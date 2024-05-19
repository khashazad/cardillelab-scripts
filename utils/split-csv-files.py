import os


def split(
    filehandler,
    delimiter=",",
    row_limit=2000,
    output_name_template="missing-lakes-%s.csv",
    output_path="/Users/khxsh/Desktop/missing-lakes/",
    keep_headers=True,
):
    import csv

    reader = csv.reader(filehandler, delimiter=delimiter)
    current_piece = 1
    current_out_path = os.path.join(output_path, output_name_template % current_piece)
    current_out_writer = csv.writer(open(current_out_path, "w"), delimiter=delimiter)
    current_limit = row_limit

    if keep_headers:
        headers = next(reader)
        current_out_writer.writerow(headers)
    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(
                output_path, output_name_template % current_piece
            )
            current_out_writer = csv.writer(
                open(current_out_path, "w"), delimiter=delimiter
            )
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)


split(open("/Users/khxsh/Downloads/missing_50cloudcover (1).csv", "r"))
