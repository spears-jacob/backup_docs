#!/usr/bin/env python
import sys
import boto3
import argparse
from argparse import RawTextHelpFormatter
from os import environ

##########################################################################################################################################################################################################################
#Setup Arg Parser
#https://stackoverflow.com/a/3041990
##########################################################################################################################################################################################################################
def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")

import logging


##########################################################################################################################################################################################################################
#Setup Arg Parser
##########################################################################################################################################################################################################################
parser = argparse.ArgumentParser(description='This script will DELETE all versions of objects in the S3 Bucket and DELETE the BUCKET  ' , formatter_class=RawTextHelpFormatter)

requiredNamed = parser.add_argument_group('required named arguments')
requiredNamed.add_argument('--bucket', required=True, help='S3 bucket name without s3://')
parser.add_argument('--profile', help='Profile from .aws/credentials to use, otherwise checks for ENV variables')
args = parser.parse_args()
delete_bucket = args.bucket

##########################################################################################################################################################################################################################
#Setup environment and logging
##########################################################################################################################################################################################################################
if not all (k in environ for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN')) and args.profile is None:
    print("Provide --profile or set environment variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN!")
    sys.exit(1)

boto3.setup_default_session(profile_name=args.profile)

logger = logging.getLogger('root')
FORMAT = "[%(asctime)s:%(lineno)3s ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)


##########################################################################################################################################################################################################################
#Check for approval and delete all object versions and delete bucket
##########################################################################################################################################################################################################################
approved = query_yes_no("Are you absolutely sure you want to NUKE all versions of objects and the entire bucket: " + delete_bucket + "?!")

if approved:   
    logger.info("Deleting All Versions and Deleting bucket: " + delete_bucket)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(delete_bucket)
    bucket.object_versions.delete()
    #bucket.delete()
else:
    logger.info("Aborting")
    logger.info("Script Finished")
    sys.exit(1)


logger.info("Script Finished")
