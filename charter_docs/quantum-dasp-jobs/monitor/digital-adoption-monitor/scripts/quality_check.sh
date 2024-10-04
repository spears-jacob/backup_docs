#!/bin/bash
export ENVIRONMENT=$1
export DASP_db=$2
export RUN_DATE=$3
export CURRENT_DAY=$4

echo "### Download jar file from s3 udf-jars ###"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/aws_common_utilities_1.0.jar ./scripts
jar -xf ./scripts/aws_common_utilities_1.0.jar
mv aws_common_utilities.sh ./scripts/
shopt -s expand_aliases
source ./scripts/aws_common_utilities.sh

export ck_outlier="select count(1) from ${DASP_db}.asp_digital_adoption_monitor_outlier where run_date = '${CURRENT_DAY}'"
export get_outlier="select * from ${DASP_db}.asp_digital_adoption_monitor_outlier where run_date = '${CURRENT_DAY}'"

echo "query is $ck_outlier"

run_athena_query "$ck_outlier" $ENVIRONMENT "pi-qtm-adhoc/output"

sendemail=0

app_limit=0
{
read
while IFS="," read -r rd_count
do
  ct=`echo $rd_count | sed 's/\"//g'`
  if [ $ct -gt $app_limit ]
  then
    sendemail=$(( $sendemail + 1 ))
  fi
done
} < "$partitionfile"

if [ $sendemail -gt 0 ]
then
  run_athena_query "$get_outlier" $ENVIRONMENT "pi-qtm-adhoc/output"

  echo "sendemail is $sendemail"

  msg="Please check job digital-adoption-monitor. Outliers are found for $RUN_DATE"
  send_email_with_attachment DL-PI-ASP-Reporting@charter.com $RUN_DATE digital-adoption-monitor "$msg" $partitionfile

  echo $msg
  #exit 1
fi
