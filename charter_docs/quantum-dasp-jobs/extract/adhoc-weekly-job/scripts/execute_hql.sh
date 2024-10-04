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
export RUN_HOUR=$6
export ADDITIONAL_PARAMS=$7
export START_DATE_MANUAL=$(echo ${ADDITIONAL_PARAMS} | jq '.START_DATE_MANUAL' | tr -d '"' | sed "s/null//" )

echo "ADDITIONAL_PARAMS is $ADDITIONAL_PARAMS"
echo "START_DATE_MANUAL is $START_DATE_MANUAL"

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
else
  export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
  export CLUSTER=${C_ID:2}
  echo "
    EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
  "

  START_COUNT=12
  END_COUNT=3
  START_DATE=`date -d "$RUN_DATE - $START_COUNT days" "+%F"`
  END_DATE=`date -d "$RUN_DATE - $END_COUNT days" "+%F"`

  if [[ ! -z ${START_DATE_MANUAL} ]]; then
    START_DATE=${START_DATE_MANUAL}
  fi

  echo RUN_DATE=$RUN_DATE
  echo START_DATE=$START_DATE
  echo END_DATE=$END_DATE

  echo ".................... Creating CQE Temp Table ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/create_cqe_temp-${SCRIPT_VERSION}.hql \
   -hiveconf START_DATE=${START_DATE} \
   -hiveconf END_DATE=${END_DATE} \
   || { echo "HQL Failure"; exit 101; }

  echo ".................... Running Outage Alerts ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/outage_alerts-${SCRIPT_VERSION}.hql \
   -hiveconf START_DATE=${START_DATE} \
   -hiveconf END_DATE=${END_DATE} \
   || { echo "HQL Failure"; exit 101; }

  echo ".................... Running CPNI Verification ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/cpni_verification-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE=${START_DATE} \
  -hiveconf END_DATE=${END_DATE} \
  || { echo "HQL Failure"; exit 101; }

  echo ".................... Dropping CQE Temp Table ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/drop_cqe_temp-${SCRIPT_VERSION}.hql \
   || { echo "HQL Failure"; exit 101; }


  echo "### Download jar file from s3 udf-jars for Quality Check ###"
  aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/aws_common_utilities_1.0.jar ./scripts
  jar -xf ./scripts/aws_common_utilities_1.0.jar
  mv aws_common_utilities.sh ./scripts/
  shopt -s expand_aliases
  source ./scripts/aws_common_utilities.sh

  echo ".................... Running Quality Check for outage_Alerts ...................."
  source ./scripts/quality_check_outage-${SCRIPT_VERSION}.sh $ENVIRONMENT $DASP_db $RUN_DATE

  echo ".................... Running Quality Check for CPNI_Verification...................."
  source ./scripts/quality_check_cpni-${SCRIPT_VERSION}.sh $ENVIRONMENT $DASP_db $RUN_DATE
fi
