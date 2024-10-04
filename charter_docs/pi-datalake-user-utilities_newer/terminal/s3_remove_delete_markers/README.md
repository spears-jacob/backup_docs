Requirements:
- Python3 (https://www.python.org/)
- boto3 (e.g., pip3 install boto3 --user)


### remove-s3-deletemarkers.py Usage
```
P2868848@CHTRMAC14TEMD6M s3_remove_delete_markers % python3 remove-s3-deletemarkers.py -h
usage: remove-s3-deletemarkers.py [-h] --bucket BUCKET --prefix PREFIX [--action ACTION] [--from_date FROM_DATE] [--to_date TO_DATE] [--profile PROFILE] [--verbose [VERBOSE]]

This script deletes the latest delete marker. It allows filtering by bucket, prefix, and time range. The time range filter is for deletion date.

optional arguments:
  -h, --help            show this help message and exit
  --action ACTION       Options: check, restore  Default: check
  --from_date FROM_DATE
                        Both dates(UTC) must be provided and date formatted like: MM/dd/yyyy H:M:S EX:2020-10-04 02:01:01
  --to_date TO_DATE     Both dates(UTC) must be provided and date formatted like: MM/dd/yyyy H:M:S EX:2020-12-04 02:01:01
  --profile PROFILE     Profile from .aws/credentials to use, otherwise checks for ENV variables
  --verbose [VERBOSE]   Shows every object being checked, default shows only Glacier type

required named arguments:
  --bucket BUCKET       S3 bucket name without s3://
  --prefix PREFIX       S3 object prefix, use '' for root