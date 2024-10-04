#!/bin/bash
export ENVIRONMENT=$1
export DASP_db=$2
export RUN_DATE=$3
export TODAY=$4

echo "### Download jar file from s3 udf-jars ###"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/aws_common_utilities_1.0.jar ./scripts
jar -xf ./scripts/aws_common_utilities_1.0.jar
mv aws_common_utilities.sh ./scripts/
shopt -s expand_aliases
source ./scripts/aws_common_utilities.sh

export YESTERDAY=`date -d "${TODAY} -1 day" +%Y-%m-%d`

echo "TODAY is ${TODAY} and YESTERDAY is ${YESTERDAY}"

export ck_yesterday="select count(1) from ${DASP_db}.asp_digital_adoption_monitor_da where date_value = '${YESTERDAY}'"

echo "query is $ck_yesterday"

run_athena_query "$ck_yesterday" $ENVIRONMENT "pi-qtm-adhoc/output"

sendemail=0

app_limit=0
{
read
while IFS="," read -r rd_count
do
  ct=`echo $rd_count | sed 's/\"//g'`
  if [ $ct -eq $app_limit ]
  then
    sendemail=$(( $sendemail + 1 ))
  fi
done
} < "$partitionfile"

if [ $sendemail -gt 0 ]
then
  run_athena_query "$get_outlier" $ENVIRONMENT "pi-qtm-adhoc/output"

  echo "sendemail is $sendemail"

  msg="Please check job digital-adoption-monitor. DATA not found for $YESTERDAY"
  send_email_with_attachment DL-PI-ASP-Reporting@charter.com $RUN_DATE digital-adoption-monitor "$msg" $partitionfile

  echo $msg
  exit 1
fi
