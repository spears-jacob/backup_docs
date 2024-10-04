#!/bin/bash
export ENVIRONMENT=$1
export EXPORT_FILE=$2

echo "### Download jar file from s3 udf-jars ###"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/aws_common_utilities_1.0.jar ./scripts
jar -xf ./scripts/aws_common_utilities_1.0.jar
mv aws_common_utilities.sh ./scripts/
source ./scripts/aws_common_utilities.sh

HASFILE=`aws s3 ls s3://com.charter.focus.prod.ingest.files/quantum/ | grep gz | grep $EXPORT_FILE | wc | awk -F ' ' '{print $1}'`
echo "HASFILE is $HASFILE"

zero=0
if [ ${HASFILE} -eq ${zero} ]; then
  msg="$EXPORT_FILE file not in s3://com.charter.focus.prod.ingest.files/quantum/"
  send_email c-anna.zheng@charter.com $RUN_DATE dasp_pmai_feed_export "$msg"

  echo $msg
  exit 1
fi
