"""
    fileHandling.py
        readCsv
        - Read and output headers from CSV file
        - Read and output records from CSV file
        writeFile
        - lines to an outputFileName
        readHqlFile
        - lines from a file

"""
import csv

def readCsv(inputFile, delimiter):
    """ readCsv
        inputFile - a file with headers
        returns a list of headers from the csv and a 2d list of rows and cols
    """
    # setup file reader
    file = open(inputFile, "rb")
    reader = csv.reader(file, delimiter=delimiter)
    # separate header from rows
    headers, rows = [], []
    getHeaders = True
    for row in reader:
        if getHeaders:
            headers = row
            getHeaders = False
        else:
            rows.append(row)
    return headers, rows


# returns a list of dictionaries using the field headers as keys for each row
def csvToDicts(inputFile, delimiter):
    file = open(inputFile, 'rb')
    reader = csv.DictReader(file, delimiter=delimiter)
    return reader

#-------------------------------------------------
# File Handling
def writeFile(outputFileName, lines):
    outputFile = open(outputFileName, "wb")
    outputFile.writelines(lines)
def readHqlFile(fileName):
    file = open(fileName, "r")
    return file.readlines()

def main(argv):
    for x in argv[1:]:
        headers, rows = readCsv(x)
        print headers
        print rows

if __name__ == "__main__":
    import sys
    main(sys.argv)
