#!/bin/bash
export ENVIRONMENT=$1
export RUN_DATE=$2

echo "### Download jar file from s3 udf-jars ###"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/aws_common_utilities_1.0.jar ./scripts
jar -xf ./scripts/aws_common_utilities_1.0.jar
mv aws_common_utilities.sh ./scripts/
source ./scripts/aws_common_utilities.sh

HASFILE=`aws s3 ls s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/ | grep gz | wc | awk -F ' ' '{print $1}'`
echo "HASFILE is $HASFILE"

zero=0
if [ ${HASFILE} -eq ${zero} ]; then
  msg="There is no PMAI input file in s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/"
  send_email c-anna.zheng@charter.com $RUN_DATE dasp_pmai_devices "$msg"

  echo $msg
  exit 1
fi
