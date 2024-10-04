#!/bin/python
# adapted from ci-utils repo, P. Khunrattanaphon, P. Shah, Z. Gendreau and many others contributing
# input from K. Kellner, J. Frye and many others

import argparse
import sys
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession

def create_session(appName):
    """Create Spark Configurations
    :param file_name: spark file name or application name
    :return: spark configurations
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(appName) \
    .enableHiveSupport()

    return spark

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sql', type=str, required=True)
    parser.add_argument('--appName', type=str, required=False, default=(os.environ.get('JOB_STEP', 'PySpark Wrapped SQL Application')))

    args = parser.parse_args()
    print("\n\nappName: {}".format(args.appName))

    with open(args.sql, 'r') as file:
        print("\n\nreading file {}".format(args.sql))
        queries = file.read().replace('\n',' ')
    # Create SparkSession
    try:
        print('\n\nStarting spark session')
        spark = create_session(args.appName).getOrCreate()

        # This prints out *all* the configurations, so it is easy to tell what was set when the session was running.
        print('\n\nPlease see *all* the configurations below at the time this session was running\n')
        set([print(c) for c in (spark.sparkContext.getConf().getAll())])

        print('\n\nStart processing queries')
        for query in queries.split(";"):
            if query.strip():
                print('\n\nQuery:')
                print(query)
                print('#' * 100)
                print ('\n\n')
                spark.sql(query)

        # Stop the spark session
        spark.stop()

        print('Processing completed successfully')
    except Exception as err:
        print(err)
        print("Unexpected error:", sys.exc_info()[0])

        exit(1)

main()