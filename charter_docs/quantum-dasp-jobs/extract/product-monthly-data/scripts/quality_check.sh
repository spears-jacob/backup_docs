#!/bin/bash
export ENVIRONMENT=$1
export DASP_db=$2
export START_DATE=$3
export END_DATE=$4
export START_TS=$(date -d "$START_DATE" '+%s')
export END_TS=$(date -d "$END_DATE" '+%s')
export RUN_DAYS=$(( ( $END_TS - $START_TS )/(60*60*24) ))

echo "### Download jar file from s3 udf-jars ###"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/aws_common_utilities_1.0.jar ./scripts
jar -xf ./scripts/aws_common_utilities_1.0.jar
mv aws_common_utilities.sh ./scripts/
shopt -s expand_aliases
source ./scripts/aws_common_utilities.sh

export qs="select count(1) from (select date_denver, count(1) row_count from $DASP_db.asp_specmobile_login where date_denver >= '$START_DATE' and date_denver < '$END_DATE' group
  by date_denver having count(1) > 0) a"

run_athena_query "$qs" $ENVIRONMENT "pi-qtm-adhoc/output"

sendemail=0

app_limit=$RUN_DAYS
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
  echo "sendemail is $sendemail"

  msg="Please check job pmai_devices_get_output. Expect number of days not equal to $app_limit for $RUN_DATE"
  send_email_with_attachment c-anna.zheng@charter.com $RUN_DATE dasp_pmai_devices_get_output "$msg" $partitionfile

  echo $msg
  exit 1
fi
