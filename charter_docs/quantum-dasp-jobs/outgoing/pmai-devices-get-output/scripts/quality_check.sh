#!/bin/bash
export ENVIRONMENT=$1
export DASP_db=$2
export RUN_DATE=$3

echo "### Download jar file from s3 udf-jars ###"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/aws_common_utilities_1.0.jar ./scripts
jar -xf ./scripts/aws_common_utilities_1.0.jar
mv aws_common_utilities.sh ./scripts/
shopt -s expand_aliases
source ./scripts/aws_common_utilities.sh

export qs="select run_date, application_name, count(1) row_count from $DASP_db.asp_pmai_sia_device_out where run_date = '$RUN_DATE' group
  by run_date, application_name"

run_athena_query "$qs" $ENVIRONMENT "pi-qtm-adhoc/output"

app_names=('MySpectrum' 'SpecMobile' 'OneApp')

sendemail=0

app_limit=0
app_count=0
{
read
while IFS="," read -r rd_label rd_app rd_count
do
  app_count=$(( $app_count + 1 ))
  date_label=`echo $rd_label | sed 's/\"//g'`
  app_name=`echo $rd_app | sed 's/\"//g'`
  ct=`echo $rd_count | sed 's/\"//g'`
  if [ $ct -eq $app_limit ]
  then
    sendemail=$(( $sendemail + 1 ))
  fi
done
} < "$partitionfile"

if [ $app_count -lt ${#app_names[@]} ]
then
  sendemail=$(( $sendemail + 1 ))
fi

if [ $sendemail -gt 0 ]
then
  echo "sendemail is $sendemail"

  msg="Please check job pmai_devices_get_output. Expect ${#app_names[@]} applications and row_count larger than $app_limit for $RUN_DATE"
  send_email_with_attachment c-anna.zheng@charter.com $RUN_DATE dasp_pmai_devices_get_output "$msg" $partitionfile

  echo $msg
  exit 0
fi
