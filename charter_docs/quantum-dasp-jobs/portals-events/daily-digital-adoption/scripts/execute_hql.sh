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

if [ "$ENVIRONMENT" == "stg" ]; then
    export BTM=${PROD_ACC_NO}/prod.experiment_btm_visit_id_account_key_lookup
    export ACCOUNTATOM=stg_dasp.asp_atom_accounts_snapshot_prod_copy
elif [ "$ENVIRONMENT" == "prod" ]; then
    export BTM=prod.experiment_btm_visit_id_account_key_lookup
    export ACCOUNTATOM=prod.atom_accounts_snapshot
fi

export SCALA_LIB=`ls /usr/lib/spark/jars/scala-library*`
echo "SCALA_LIB is ${SCALA_LIB}"

export CADENCE=daily

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

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

export START_DATE_DENVER=`date -d "$START_DATE -8 day" +%Y-%m-%d`
#export END_DATE_DENVER=$START_DATE_DENVER #for 1 day
export END_DATE_DENVER=$START_DATE  #for 8 days

echo START_DATE: $START_DATE
echo END_DATE: $END_DATE
echo START_DATE_DENVER: $START_DATE_DENVER
echo END_DATE_DENVER: $END_DATE_DENVER
echo CADENCE: $CADENCE
echo EXEC_ID: ${exec_id}
echo STEP_ID: $STEP

echo "### Creating all views with hive..."
bash ./scripts/create_hive_views-${SCRIPT_VERSION}.sh

echo "### Getting ${CADENCE} Atom Data...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/1_atom-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_DENVER="${START_DATE_DENVER}" -hiveconf END_DATE_DENVER="${END_DATE_DENVER}" \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
 || { echo "HQL Failure"; exit 101; }

echo "### Getting ${CADENCE} Portals Data...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/2_portals-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_DENVER="${START_DATE_DENVER}" -hiveconf END_DATE_DENVER="${END_DATE_DENVER}" \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
 || { echo "HQL Failure"; exit 101; }

echo "### Getting ${CADENCE} Call Data...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/3_call-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_DENVER="${START_DATE_DENVER}" -hiveconf END_DATE_DENVER="${END_DATE_DENVER}" \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
 || { echo "HQL Failure"; exit 101; }

echo "### Getting ${CADENCE} Final Data for Digital Adoption...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/4_combine-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_DENVER="${START_DATE_DENVER}" -hiveconf END_DATE_DENVER="${END_DATE_DENVER}" \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
 || { echo "HQL Failure"; exit 101; }
