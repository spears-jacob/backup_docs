Requirements:
- Python3 (https://www.python.org/)
- boto3 (e.g., pip3 install boto3 --user)
- beautifulsoup4 (e.g., pip3 install beautifulsoup4 --user)
- requests(e.g., pip3 install requests --user)


### glacier_restore.py Usage
```
$ python3 glacier-restore.py -h
usage: glacier-restore.py [-h] --bucket BUCKET --prefix PREFIX
                          [--action ACTION]
                          [--target_storage_class TARGET_STORAGE_CLASS]
                          [--retrieval_tier RETRIEVAL_TIER] [--days DAYS]
                          [--profile PROFILE] [--verbose [VERBOSE]]

This script supports checking status of in progress Glacier restorations, submitting Glacier restorations, and copying the restored Glacier objects to a new tier.
If the source object's storage class is GLACIER, you must restore a copy of this object before you can use it as a source object for the copy operation

optional arguments:
  -h, --help            show this help message and exit
  --action ACTION       Glacier objects must be restored before copying to higher level tier. Options: check, restore, copy  Default: check
  --target_storage_class TARGET_STORAGE_CLASS
                        The type of storage to use for the restored object. Only for *copy* action. Options: STANDARD, REDUCED_REDUNDANCY, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE  Default: STANDARD
  --retrieval_tier RETRIEVAL_TIER
                        Restoration request Tier, these also change cost. Only for *restore* action. Options: Expedited (1-5mins), Standard (3-5hrs), Bulk (5-12hrs)  Default: STANDARD
  --days DAYS           Number of days before object is unrestored
  --profile PROFILE     Profile from .aws/credentials to use, otherwise checks for ENV variables
  --verbose [VERBOSE]   Shows every object being checked, default shows only Glacier type

required named arguments:
  --bucket BUCKET       S3 bucket name without s3://
  --prefix PREFIX       S3 object prefix, use '' for root