"""
    file_handling.py
        read_csv
        - Read and output headers from CSV file
        - Read and output records from CSV file
        write_file
        - lines to an output_file_name
        read_hql_file
        - lines from a file

"""
import csv


def read_csv(input_file, delimiter):
    """ read_csv
        input_file - a file with headers
        returns a list of headers from the csv and a 2d list of rows and cols
    """
    # setup file reader
    file = open(input_file, "r")
    reader = csv.reader(file, delimiter=delimiter)
    # separate header from rows
    headers, rows = [], []
    get_headers = True
    for row in reader:
        if get_headers:
            headers = row
            get_headers = False
        else:
            rows.append(row)
    return headers, rows


# returns a list of dictionaries using the field headers as keys for each row
def csv_to_dicts(input_file, delimiter):
    file = open(input_file, 'r')
    reader = csv.DictReader(file, delimiter=delimiter)
    return reader


# -------------------------------------------------
# File Handling
def write_file(output_file_name, lines):
    output_file = open(output_file_name, "w")
    output_file.writelines(lines)


def read_hql_file(file_name):
    file = open(file_name, "r")
    return file.readlines()


def main(argv, delimiter):
    for x in argv[1:]:
        headers, rows = read_csv(x, delimiter=delimiter)
        print(headers)
        print(rows)
