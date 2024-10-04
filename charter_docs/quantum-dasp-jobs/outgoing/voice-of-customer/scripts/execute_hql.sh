#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export JOB_STEP_NAME=$(echo $5 | cut -d"-" -f 1)
export SCRIPT_VERSION=$(echo $5 | cut -d"-" -f 2 | cut -d"." -f 1)
export PROD_ACC_NO=387455165365
export S3_SECURE_SSPP="s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/voice_of_customer"


export CADENCE=daily

export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

if [ "$ENVIRONMENT" == "stg" ]; then
    export CQE=${PROD_ACC_NO}/prod.core_quantum_events_sspp
    export MVNO_A=${PROD_ACC_NO}/prod_dasp.mvno_accounts
elif [ "$ENVIRONMENT" == "prod" ]; then
    export CQE=prod.core_quantum_events_sspp
    export MVNO_A=prod_dasp.mvno_accounts
fi

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
echo END_DATE : $END_DATE
echo START_DATE_TZ: $START_DATE_TZ
echo END_DATE_TZ : $END_DATE_TZ
FD=$(echo "${RUN_DATE//-}")
echo FD: ${FD}

echo "
### Job Step Name echo:
  -> \${JOB_STEP_NAME} = ${JOB_STEP_NAME}
"

if [ "${JOB_STEP_NAME}" == "1_asp_extract_voice_of_customer_troubleshooting" ]; then
  echo "### Extracting VoC cable data ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/asp_extract_voice_of_customer_troubleshooting-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "2_asp_extract_voice_of_customer_mobile_troubleshooting" ]; then
  echo "### Extracting VoC mobile troubleshooting data ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/asp_extract_voice_of_customer_mobile_troubleshooting-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }

elif [ "${JOB_STEP_NAME}" == "3_outgoing_secure_voice_of_customer_troubleshooting" ]; then
  echo "### Writing Outgoing VoC Troubleshooting data ....................
  "
  hive -S -v -f ${ARTIFACTS_PATH}/hql/outgoing_secure_voice_of_customer_troubleshooting-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
  mv 000000_0 voice_of_customer_troubleshooting_${FD}.csv ;
  echo "
  voice_of_customer_troubleshooting_${FD}.csv has $(cat voice_of_customer_troubleshooting_${FD}.csv | wc -l) rows
  "
  ls -alh voice_of_customer_troubleshooting_${FD}.csv ;
  echo -e "\n### Moving outgoing VoC Troubleshooting data to secure location....................\n"
  aws s3 cp voice_of_customer_troubleshooting_${FD}.csv $S3_SECURE_SSPP/troubleshooting/cable/voice_of_customer_troubleshooting_${FD}.csv
  aws s3 ls $S3_SECURE_SSPP/troubleshooting/cable/voice_of_customer_troubleshooting_${FD}.csv
  if [ $? -ne 0 ]; then echo -e "\n\tIt looks like the file did not transfer successfully"; kill -INT $$; fi

elif [ "${JOB_STEP_NAME}" == "4_outgoing_secure_voice_of_customer_mobile_troubleshooting" ]; then
  echo "### Writing Outgoing VoC Mobile Troubleshooting data ....................
  "
  hive -S -v -f ${ARTIFACTS_PATH}/hql/outgoing_secure_voice_of_customer_mobile_troubleshooting-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
  mv 000000_0 voice_of_customer_mobile_troubleshooting_${FD}.csv ;
  echo "
  voice_of_customer_mobile_troubleshooting_${FD}.csv has $(cat voice_of_customer_mobile_troubleshooting_${FD}.csv | wc -l) rows
  "
  ls -alh voice_of_customer_mobile_troubleshooting_${FD}.csv ;
  echo -e "\n### Moving outgoing VoC Mobile Troubleshooting data to secure location....................\n"
  aws s3 cp voice_of_customer_mobile_troubleshooting_${FD}.csv $S3_SECURE_SSPP/troubleshooting/mobile/voice_of_customer_mobile_troubleshooting_${FD}.csv
  aws s3 ls $S3_SECURE_SSPP/troubleshooting/mobile/voice_of_customer_mobile_troubleshooting_${FD}.csv
  if [ $? -ne 0 ]; then echo -e "\n\tIt looks like the file did not transfer successfully"; kill -INT $$; fi
fi
