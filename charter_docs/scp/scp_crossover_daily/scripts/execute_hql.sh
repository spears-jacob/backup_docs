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
export CADENCE=fiscal_monthly
export PROD_ACC_NO=387455165365
export STG_ACC_NO=213705006773

if [ "$ENVIRONMENT" == "stg" ]; then
    export BAES=${PROD_ACC_NO}/prod_ciwifi.billing_acct_equip_snapshot
    export LOGIN=${PROD_ACC_NO}/prod_dasp.asp_scp_portals_login

elif [ "$ENVIRONMENT" == "prod" ]; then
    export BAES=prod_ciwifi.billing_acct_equip_snapshot
    export LOGIN=prod_dasp.asp_scp_portals_login
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
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;


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

if   [ "${JOB_STEP_NAME}" == "1_scp_portals_product" ]; then
    echo "### Running SCP Portals daily action extract, scp_portals_action  ..................."
    hive -S -v -f ${ARTIFACTS_PATH}/hql/1_load_scp_portals_product-${SCRIPT_VERSION}.hql || { echo "HQL Failure load_scp_portals_action"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
  echo "### Refreshing tableau  ..................."
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
fi
