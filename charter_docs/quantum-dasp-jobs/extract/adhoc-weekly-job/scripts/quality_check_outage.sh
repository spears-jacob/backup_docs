#!/bin/bash
export ENVIRONMENT=$1
export DASP_db=$2
export RUN_DATE=$3

export qs="select partition_date_utc, count(1) row_count from $DASP_db.asp_adhoc_outage_alerts where partition_date_utc >= '$START_DATE' group
 by partition_date_utc order by partition_date_utc"

run_athena_query "$qs" $ENVIRONMENT "pi-qtm-adhoc/output"

sendemail=0
num_days=10

app_limit=0
app_count=0
{
read
while IFS="," read -r rd_date rd_count
do
  app_count=$(( $app_count + 1 ))
  date_label=`echo $rd_date | sed 's/\"//g'`
  ct=`echo $rd_count | sed 's/\"//g'`
  if [ $ct -eq $app_limit ]
  then
    sendemail=$(( $sendemail + 1 ))
  fi
done
} < "$partitionfile"

if [ $app_count -lt ${num_days} ]
then
  sendemail=$(( $sendemail + 1 ))
fi

if [ $sendemail -gt 0 ]
then
  echo "sendemail is $sendemail"

  msg="Please check job adhoc-weekly-job: outage_alters. Expect ${num_days} days and row_count larger than $app_limit for $RUN_DATE"
  send_email_with_attachment c-anna.zheng@charter.com $RUN_DATE dasp_adhoc_weekly_job "$msg" $partitionfile

  echo $msg
  exit 1
fi
