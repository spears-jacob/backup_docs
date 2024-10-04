#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export PROD_ACC_NO=387455165365

if [ "$ENVIRONMENT" == "stg" ]; then
    export CPV=${PROD_ACC_NO}/prod_dasp.cs_calls_with_prior_visits
elif [ "$ENVIRONMENT" == "prod" ]; then
    export CPV=prod_dasp.cs_calls_with_prior_visits
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


hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

END_DATE=`date -d "$RUN_DATE -2 day" +%Y-%m-%d` #call data is 2 days in arrears
LOAD_DATE=`date -d "$END_DATE -37 day" +%Y-%m-%d` #Nuances in how CQE is partitioned means that sometimes partitions aren't in the expected date range.  Testing shows that including this far back is a negligible increase in computing power, and is a guaranteed-safe date range
VISIT_START_DATE=`date -d "$LOAD_DATE -3 day" +%Y-%m-%d` #visits may match to calls up to 2 patition days before; 3 keeps us extra safe

echo "Running for "$RUN_DATE
echo "Which means loading data since "$LOAD_DATE
echo "Which might match to visits since "$VISIT_START_DATE

hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_selfservice_metagg_call_enrich-${SCRIPT_VERSION}.hql -hiveconf visit_start_date=$VISIT_START_DATE -hiveconf call_start_date=$LOAD_DATE -hiveconf end_date=$RUN_DATE || { echo "HQL Failure"; exit 101; }
