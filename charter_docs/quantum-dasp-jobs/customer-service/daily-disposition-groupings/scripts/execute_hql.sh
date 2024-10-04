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

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

PRIOR_MONTH_START_DATE=`date -d "$RUN_DATE -1 month" +%Y-%m-01`
PRIOR_MONTH_END_DATE=`date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days" +%Y-%m-%d`

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./

if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
else

  echo "### Inserting a new row in to cs_dispositions for any new issues, causes, or resolutions...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/update_dispositions-${SCRIPT_VERSION}.hql \
   -hiveconf prior_month_start_date="${PRIOR_MONTH_START_DATE}" \
   -hiveconf prior_month_end_date="${PRIOR_MONTH_END_DATE}"\
	 -hiveconf CLUSTER=$CLUSTER || { echo "HQL Failure"; exit 101; }


  echo "### Creating table of issues and their groupings...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/create_disposition_groupings-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }

fi
