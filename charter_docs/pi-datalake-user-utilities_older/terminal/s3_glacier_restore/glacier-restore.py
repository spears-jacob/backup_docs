#!/usr/local/bin/python3

#*************************************************************************************************************************************************************************************************
# Resources
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#restore-glacier-objects-in-an-amazon-s3-bucket
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
#*************************************************************************************************************************************************************************************************

import argparse
from argparse import RawTextHelpFormatter
from os import environ
import sys
import boto3
import json
import logging

##########################################################################################################################################################################################################################
#Setup Arg Parser
parser = argparse.ArgumentParser(description='This script supports checking status of in progress Glacier restorations, submitting Glacier restorations, and copying the restored Glacier objects to a new tier.  ' 
             '\nIf the source object\'s storage class is GLACIER, you must restore a copy of this object before you can use it as a source object for the copy operation', formatter_class=RawTextHelpFormatter)

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
parser.add_argument('--dest_bucket', help='Used restoring object to differenet S3 bucket name without s3://')
parser.add_argument('--action', type = str.lower, default='check', help='Glacier objects must be restored before copying to higher level tier. Options: check, restore, copy  Default: check')
parser.add_argument('--filter', help='This filters when prefix filtering does not work. Case sensitive. Accepts comma seperated list . Options: string')
parser.add_argument('--target_storage_class', type = str.upper,  default='STANDARD', help='The type of storage to use for the restored object. Only for *copy* action. Options: STANDARD, REDUCED_REDUNDANCY, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE  Default: STANDARD')
parser.add_argument('--retrieval_tier', type = str.capitalize, default='Bulk', help='Restoration request Tier, these also change cost. Only used *restore* action. Options: Expedited (1-5mins), Standard (3-5hrs), Bulk (5-12hrs)  Default: Bulk')
parser.add_argument('--days', default=3, help='Number of days before object is unrestored')
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
target_storage_class = args.target_storage_class
retrieval_tier = args.retrieval_tier
days = args.days
verbose = args.verbose
#If destination location is set use that, otherwise copy in-place
if args.dest_bucket:
    dest_bucket = args.dest_bucket
else:
    dest_bucket = args.bucket

if args.filter:
    filter_list = [x.strip() for x in args.filter.split(",")]

boto3.setup_default_session(profile_name=args.profile)
client = boto3.client('s3')
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
# This function paginates through the list_objects_v2 api (1000 objects at a time)
def get_all_s3_objects(s3, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')
##########################################################################################################################################################################################################################
 
##########################################################################################################################################################################################################################
# Uses python generator to iterate through get_all_s3_objects and checks object class. If it is GLACIER, based on the chosen action, it will glacier restore or cp the object to chosen storage class.
for file in get_all_s3_objects(boto3.client('s3'), Bucket=bucket, Prefix=prefix):
    key=file['Key']
    #if filter is set, continue to next iteration if key doesn't contain the filter
    if 'filter_list' in locals():
        if not any(substring in key for substring in filter_list):
            logger.debug("Skipping object " + key + ", filtered out due to filter: " + args.filter)
            continue
    if file['StorageClass'] == 'GLACIER' or file['StorageClass'] == 'DEEP_ARCHIVE':
        logger.info("Checking object %s, storage class %s"  % (key,file['StorageClass']))
        obj2 = client.head_object(Bucket=bucket, Key=key)
        logger.debug("Output: %s" % obj2)
        # Try to restore the object if the storage class is glacier and
        # the object does not have a completed or ongoing restoration
        # request.
        if 'Restore' not in obj2:
            if action == 'restore':
                response = client.restore_object(
                    Bucket=bucket, 
                    Key=key, RestoreRequest={
                        'Days': int(days),
                        'GlacierJobParameters': {
                            'Tier': retrieval_tier,
                        },
                    },
                )
                logger.info('Restoration Request submitted for: %s' % key)
                logger.debug("Output: %s" % response)
            else:
                logger.info('Object is in Glacier: %s' % key)
        # Print out objects whose restoration is on-going
        elif 'ongoing-request="true"' in obj2['Restore']:
            logger.info('Restoration in-progress: %s' % key)
        # Print out objects whose restoration is complete
        elif 'ongoing-request="false"' in obj2['Restore']:
            logger.info('Restoration complete, ready to copy: %s' % key)

            if action == 'copy':
                logger.info('Copying: %s' % key)
                copy_source = {
                    'Bucket': bucket,
                    'Key': key 
                }

                response = client.copy_object(CopySource=copy_source,Bucket=dest_bucket,Key=key, StorageClass=target_storage_class, MetadataDirective='COPY') 
                logger.debug(response)
    else:
        logger.debug("Skipping object %s, storage class %s"  % (key,file['StorageClass']))
##########################################################################################################################################################################################################################

logger.info("Script Finished")
