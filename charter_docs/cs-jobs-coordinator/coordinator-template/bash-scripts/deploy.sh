#!/bin/bash
set -x

ARTIFACTS_BUCKET=$1
S3_ARTIFACTS_DIR=$2

apk add py3-setuptools
pip3 install --upgrade awscli

for ARCHIVE_FILE in $(find $(pwd) -type f -name "*.zip"); do
    aws s3 cp "${ARCHIVE_FILE}" "s3://${ARTIFACTS_BUCKET}/$S3_ARTIFACTS_DIR/"
done
