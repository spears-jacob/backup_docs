#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo RUN_DATE: $RUN_DATE

echo "### List PMAI File of SIA Export Site...................."
aws s3 ls s3://com.charter.focus.prod.export.files/quantum-list-match/

HASFILE=`aws s3 ls s3://com.charter.focus.prod.export.files/quantum-list-match/ | grep gz | wc | awk -F ' ' '{print $1}'`
echo "HASFILE is $HASFILE"

zero=0
if [ ${HASFILE} -eq ${zero} ]; then
  echo "There is no PMAI input file"
  exit 1
fi

echo "### List PMAI File of Secured Bucket Before copy...................."
aws s3 ls s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/

echo "### Move Secured Bucket Old PMAI File to Archive Folder ...................."
aws s3 mv s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/ \
          s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/archive/asp_pmai_sia_account_in/ \
          --recursive

echo "### Copy PMAI File to Secured Bucket ...................."
aws s3 cp s3://com.charter.focus.prod.export.files/quantum-list-match/ \
    s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/ \
    --recursive --exclude "*" --include "*.dat.gz"

echo "### List PMAI File of Secured Bucket After copy...................."
aws s3 ls s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/

echo "### Run Quality Check ...................."
. ./scripts/quality_check-${SCRIPT_VERSION}.sh $ENVIRONMENT $RUN_DATE
