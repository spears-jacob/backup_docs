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
export LAG_DAYS=2
export CADENCE=daily
export PROD_ACC_NO=387455165365
export STG_ACC_NO=213705006773

if [ "$ENVIRONMENT" == "stg" ]; then
    export CQES=${PROD_ACC_NO}/prod.core_quantum_events_sspp
    export BAES=${PROD_ACC_NO}/prod_ciwifi.billing_acct_equip_snapshot
    export EQUIP=${PROD_ACC_NO}/prod.atom_snapshot_equipment_20190201
    export BILLAGG=stg_dasp.asp_scp_portals_billing_agg
    export LOGSA=stg_ciwifi.scp_portals_login_subscriber_agg
    export BSA=${PROD_ACC_NO}/prod_ciwifi.scp_billing_subscriber_agg
elif [ "$ENVIRONMENT" == "prod" ]; then
    export CQES=prod.core_quantum_events_sspp
    export BAES=prod_ciwifi.billing_acct_equip_snapshot
    export EQUIP=prod.atom_snapshot_equipment_20190201
    export BILLAGG=prod_dasp.asp_scp_portals_billing_agg
    export LOGSA=${STG_ACC_NO}/stg_ciwifi.scp_portals_login_subscriber_agg
    export BSA=${PROD_ACC_NO}/prod_ciwifi.scp_billing_subscriber_agg
fi

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

export ROLLING_30_START_DATE=$(date -d "$START_DATE - 30 day" +%F)

echo TIME_ZONE: $TZ
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;
echo ROLLING_30_START_DATE: $ROLLING_30_START_DATE;

#############  Get unique IDs that will prevent tablename conflicts ############
export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
 EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"
export exec_id=$CLUSTER

export pwd=`pwd`
echo -e "\n\n### Execution is occurring the following directory:  $pwd\n\n"

# The step id needs to be divided from the pwd directory using the below approach
# because the length of the step identifier varies.
export STEP=${pwd#*-}
echo "
  EMR STEP ID: $STEP (not including the S- prefix)
"

if   [ "${JOB_STEP_NAME}" == "1_scp_portals_daily_action_extract_from_cqesspp" ]; then
    echo "### Running SCP Portals daily action extract, scp_portals_action  ..................."
    hive -S -v -f ${ARTIFACTS_PATH}/hql/1_load_scp_portals_action-${SCRIPT_VERSION}.hql || { echo "HQL Failure load_scp_portals_action"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "2_scp_account_agg_from_action_extract" ]; then
  echo "### Running SCP Portals account aggregate from action extract  ..................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/2_load_scp_portals_acct_agg-${SCRIPT_VERSION}.hql || { echo "HQL Failure load_scp_portals_acct_agg"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "3_scp_actions_agg_30daylag_from_action_extract" ]; then
  echo "### Running SCP Portals action aggregate with 30 day lag from action extract ..................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/3_load_scp_portals_actions_agg-${SCRIPT_VERSION}.hql || { echo "HQL Failure load_scp_portals_actions_agg"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "4_load_scp_portals_billing_agg" ]; then
  echo "### Running SCP Portals billing aggregate ..................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/4_load_scp_portals_billing_agg-${SCRIPT_VERSION}.hql || { echo "HQL Failure load_scp_portals_actions_agg"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "5_scp_subscriber_agg" ]; then
  echo "### Running SCP Portals subscriber aggregate ..................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/5_load_scp_portals_subscriber_agg-${SCRIPT_VERSION}.hql || { echo "HQL Failure load_scp_portals_actions_agg"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "6_tableau_refresh" ]; then
  echo "### Refreshing tableau  ..................."
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
fi
