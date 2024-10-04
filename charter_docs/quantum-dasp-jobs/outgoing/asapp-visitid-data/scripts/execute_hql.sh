#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export JOB_STEP=$5
export JOB_STEP_NAME=$(echo $5 | cut -d"-" -f 1)
export SCRIPT_VERSION=$(echo $5 | cut -d"-" -f 2 | cut -d"." -f 1)

export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"
export CADENCE=daily
export LAG_DAYS=4
export LEAD_DAYS=2
export feed_recipient=bi_shared_services
export feed_purpose=asapp_visitid_data
export S3_SECURE_SSPP="s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/${feed_recipient}"

if [ $ENVIRONMENT != prod ]
then
export PROD_ONLY_TABLE_LOCATION=387455165365/
fi

echo "prod_only_table_location = $PROD_ONLY_TABLE_LOCATION"

#############  Get unique IDs that will prevent tablename conflicts ############
export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
 EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"
export exec_id=$CLUSTER

export pwd=`pwd`
echo -e "\n\n### Execution is occurring the following directory:  $pwd\n\n"

# The step needs to be divided from the pwd directory using the below approach
# because the length of the step identifier varies.
export STEP=${pwd#*-}
echo "
  EMR STEP ID: $STEP (not including the S- prefix)
"

#################### Start Actual Code ########################

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo TIME_ZONE: $TZ
echo START_DATE: $START_DATE
echo END_DATE: $END_DATE
echo START_DATE_TZ: $START_DATE_TZ
echo END_DATE_TZ: $END_DATE_TZ

echo "### Copy Key File ...................."
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "JOB_STEP_NAME IS ${JOB_STEP_NAME}"

if [ "${JOB_STEP_NAME}" == "1_asp_extract_asapp_visitid_data_daily" ]; then
  echo "### Running asapp_visitid_data daily processing job ...................."
  # use this approach if dates are needed in the HQL, such as for a daily process.
  hive -S -v -f ${ARTIFACTS_PATH}/hql/asp_extract_asapp_visitid_data_daily-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE_TZ=$START_DATE_TZ \
  -hiveconf END_DATE_TZ=$END_DATE_TZ \
  -hiveconf START_DATE=$START_DATE \
  -hiveconf execid=$exec_id \
  -hiveconf stepid=$STEP \
  -hiveconf prod_only_table_location=$PROD_ONLY_TABLE_LOCATION \
  -hiveconf END_DATE=$END_DATE || { echo "HQL Failure"; exit 101; }

elif [ "${JOB_STEP_NAME}" == "2_outgoing_asapp_visitid_data" ]; then
  export outfile="asp_extract_asapp_visitid_data_daily_pvt_${START_DATE}_spa.tsv"
  echo "### Writing Outgoing ${feed_recipient} ${feed_purpose} data ....................
  "
  hive -S -v -f ${ARTIFACTS_PATH}/hql/outgoing_secure_asapp_visitid_data-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
  mv 000000_0 ${outfile};
  # adds header  sed -i -e '1i"application_name","acct_number","survey_action","biller_type","division","division_id","sys","prin","agent","acct_site_id","account_company","acct_franchise","day_diff","rpt_dt","partition_date_utc"' ${outfile};
  echo "
  ${outfile} has $(cat ${outfile} | wc -l) rows
  "
  ls -alh ${outfile} ;
  echo -e "\n### Moving Outgoing ${feed_recipient} ${feed_purpose} data to secure location....................\n"
  aws s3 cp ${outfile} ${S3_SECURE_SSPP}/${feed_purpose}/${outfile}
  aws s3 ls ${S3_SECURE_SSPP}/${feed_purpose}/${outfile}
if [ $? -ne 0 ]; then echo -e "\n\tIt looks like the file ${outfile} did not transfer successfully"; kill -INT $$; fi
fi
