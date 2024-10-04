#!/usr/bin/python3

#*************************************************************************************************************************************************************************************************
# Resources
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
#*************************************************************************************************************************************************************************************************

import argparse
from argparse import RawTextHelpFormatter
from os import environ
import sys
import boto3
import json
import logging
import dateutil
import datetime
import pytz

##########################################################################################################################################################################################################################
#Setup Arg Parser
parser = argparse.ArgumentParser(description='This script deletes the latest delete marker. It allows filtering by bucket, prefix, and time range. The time range filter is for deletion date.', formatter_class=RawTextHelpFormatter)

#Define argparse bool check
def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

requiredNamed = parser.add_argument_group('required named arguments')
requiredNamed.add_argument('--bucket', required=True, help='S3 bucket name without s3://')
requiredNamed.add_argument('--prefix', required=True, help='S3 object prefix, use \'\' for root')
parser.add_argument('--action', type = str.lower, default='check', help='Options: check, restore  Default: check')
parser.add_argument('--from_date', type = str.lower, help='Both dates(UTC) must be provided and date formatted like: MM/dd/yyyy H:M:S EX:2020-10-04 02:01:01')
parser.add_argument('--to_date', type = str.lower, help='Both dates(UTC) must be provided and date formatted like: MM/dd/yyyy H:M:S EX:2020-12-04 02:01:01')
parser.add_argument('--profile', help='Profile from .aws/credentials to use, otherwise checks for ENV variables')
parser.add_argument('--verbose', type=str2bool, nargs='?', const=True, default=0, help='Shows every object being checked, default shows only Glacier type')
args = parser.parse_args()
##########################################################################################################################################################################################################################

##########################################################################################################################################################################################################################
#Setup environment and logging
if not all (k in environ for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN')) and args.profile is None:
    print("Provide --profile or set environment variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN!")
    sys.exit(1)

bucket = args.bucket
prefix = args.prefix
action = args.action
verbose = args.verbose

#Validate dates if provided
if args.from_date and args.to_date:
    from_date = dateutil.parser.parse(args.from_date)
    from_date = pytz.utc.localize(from_date)
    to_date = dateutil.parser.parse(args.to_date)
    to_date = pytz.utc.localize(to_date)
    print("Date Filter Parameters: FROM " + str(from_date) + " TO " + str(to_date) + "\n")


#Setup AWS
boto3.setup_default_session(profile_name=args.profile)
s3 = boto3.resource('s3')
s3client = boto3.client('s3')

#Setup Logger
logger = logging.getLogger('root')
FORMAT = "[%(asctime)s:%(lineno)3s ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)
if verbose:
    logger.setLevel(logging.DEBUG)
logger.debug('***********************VERBOSE***********************')
##########################################################################################################################################################################################################################

##########################################################################################################################################################################################################################
# Remove the latest delete markers for the specified bucket/prefix.

paginator = s3client.get_paginator('list_object_versions')
pageresponse = paginator.paginate(Bucket=bucket, Prefix=prefix)

# iter over the pages from the paginator
for pageobject in pageresponse:
    # Find if there are any delmarkers
    if 'DeleteMarkers' in pageobject.keys():
        for each_delmarker in pageobject['DeleteMarkers']:
            if each_delmarker['IsLatest']:
                if args.from_date and args.to_date:
                    object_date=each_delmarker['LastModified']
                    # Check that object is within the restore timeframe
                    if object_date > from_date and object_date < to_date:
                    # Create a resource for the version-object and use .delete() to remove it.
                        if action == 'restore':
                            fileobjver = s3.ObjectVersion(
                                bucket,
                                each_delmarker['Key'],
                                each_delmarker['VersionId']
                            )                        
                            print('Restoring ' + each_delmarker['Key']  + " - Last Modified Date: " + str(object_date))
                            fileobjver.delete()
                        else:
                            print('Check mode enabled, need to restore object: ' + each_delmarker['Key'] + " - Last Modified Date: " + str(object_date))
                else:
                    print(each_delmarker['LastModified'])
                    # Create a resource for the version-object and use .delete() to remove it.
                    fileobjver = s3.ObjectVersion(
                        bucket,
                        each_delmarker['Key'],
                        each_delmarker['VersionId']
                    )
                    if action == 'restore':
                        fileobjver = s3.ObjectVersion(
                            bucket,
                            each_delmarker['Key'],
                            each_delmarker['VersionId']
                        )
                        print('Restoring ' + each_delmarker['Key'])
                        fileobjver.delete()
                    else:
                        print('Check mode enabled, need to restore object: ' + each_delmarker['Key'])
