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
export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"
export CADENCE=daily
export LAG_DAYS=2
export PROD_ACC_NO=387455165365

if [ "$ENVIRONMENT" == "stg" ]; then
    export PCQE=${PROD_ACC_NO}/prod.core_quantum_events_sspp
		export ACCC=${PROD_ACC_NO}/prod.atom_cs_call_care_data_3
		export ASQE=${PROD_ACC_NO}/prod_dasp.asp_support_quantum_events
elif [ "$ENVIRONMENT" == "prod" ]; then
		export PCQE=prod.core_quantum_events_sspp
		export ACCC=prod.atom_cs_call_care_data_3
		export ASQE=prod_dasp.asp_support_quantum_events
fi

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
else
  echo "### Pulling down UDF cipher keys..."
  s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

  echo "### Making search support performance views..."
  bash ./scripts/create_views-${SCRIPT_VERSION}.sh

  echo "### Dates process - Download shell script"
  aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

  echo "### Dates process - Extract shell script and timezone lookup from jar file"
  jar -xfM process_dates.jar

  echo "### Dates process - Process dates"
  source process_dates.sh

echo "### Adding supplementary custom dates"
export STARTDATE_3day=`date --date="$START_DATE - 3 day" +%Y-%m-%d`
export END_DATE_1day=`date --date="$END_DATE + 1 day" +%Y-%m-%d`

  echo "Running Support Content Agg......"
  echo TIME_ZONE: $TZ
  echo START_DATE: $START_DATE
  echo END_DATE: $END_DATE
  echo START_DATE_TZ: $START_DATE_TZ
  echo END_DATE_TZ: $END_DATE_TZ
  echo DaysToLag: $LAG_DAYS
  echo STARTDATE_3day: $STARTDATE_3day
  echo END_DATE_1day: $END_DATE_1day

  export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
  export CLUSTER=${C_ID:2}
  echo "
    EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
  "

  hive -S -v -f ${ARTIFACTS_PATH}/hql/support_search-${SCRIPT_VERSION}.hql \
      -hiveconf START_DATE=${START_DATE} \
      -hiveconf START_DATE_TZ=${START_DATE_TZ} \
      -hiveconf END_DATE=${END_DATE} \
      -hiveconf END_DATE_TZ=${END_DATE_TZ} \
      -hiveconf DaysToLag=${LAG_DAYS} \
      || { echo "HQL Failure"; exit 101; }
fi
