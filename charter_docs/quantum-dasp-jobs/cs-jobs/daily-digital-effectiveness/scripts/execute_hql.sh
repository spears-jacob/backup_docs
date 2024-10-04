#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export RUN_HOUR=$6
export ADDITIONAL_PARAMS=$7
export PROD_ACC_NO=387455165365

echo "ADDITIONAL_PARAMS = ${ADDITIONAL_PARAMS}"
export END_DATE_MANUAL=$(echo ${ADDITIONAL_PARAMS} | jq '.END_DATE_MANUAL' | tr -d '"' | sed "s/null//" )

if [ "$ENVIRONMENT" == "stg" ]; then
    export CALLVISIT=${PROD_ACC_NO}/prod_dasp.cs_calls_with_prior_visits
elif [ "$ENVIRONMENT" == "prod" ]; then
    export CALLVISIT=prod_dasp.cs_calls_with_prior_visits
fi

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

export CADENCE=daily

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

export START_DATE_UTC=`date -d "$START_DATE -1 day" +%Y-%m-%d`
export END_DATE_UTC=`date -d "$END_DATE -2 day" +%Y-%m-%d`

if [[ ! -z ${END_DATE_MANUAL} ]]; then
  export START_DATE_UTC=${START_DATE}
  export END_DATE_UTC=${END_DATE_MANUAL}
fi

echo START_DATE_UTC: $START_DATE_UTC
echo END_DATE_UTC: $END_DATE_UTC
echo GRAIN: $GRAIN

echo "### Running ${CADENCE} CIR Drilldown...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/cir_drilldown-${SCRIPT_VERSION}.hql \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
 -hiveconf START_DATE_UTC="${START_DATE_UTC}" \
 -hiveconf END_DATE_UTC="${END_DATE_UTC}" || { echo "HQL Failure"; exit 101; }

if [[ -z ${END_DATE_MANUAL} ]]; then
  echo "### Running ${CADENCE} CIR Visit Drilldown...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/cir_visit_drilldown-${SCRIPT_VERSION}.hql \
   -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
   -hiveconf START_DATE_UTC="${START_DATE_UTC}" \
   -hiveconf END_DATE_UTC="${END_DATE_UTC}" || { echo "HQL Failure"; exit 101; }
fi
