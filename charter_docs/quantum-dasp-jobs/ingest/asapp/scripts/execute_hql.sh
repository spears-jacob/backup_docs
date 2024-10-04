#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export SEC_db=${ENVIRONMENT}_sec_repo_sspp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export RUN_HOUR=$6
export ADDITIONAL_PARAMS=$7
export CADENCE=daily

export RUN_INIT=$(echo ${ADDITIONAL_PARAMS} | jq '.RUN_INIT' | tr -d '"' | sed "s/null//" )
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

echo "### Pulling down UDF cipher keys..."
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

if [ "$RUN_INIT" = "Yes" ]; then
   echo "### Creating sec tables with hive..."
   bash ./scripts/create_sec_tables-${SCRIPT_VERSION}.sh
fi;

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

echo RUN_DATE: $RUN_DATE
echo EXEC_ID: ${exec_id}
echo STEP_ID: $STEP

echo "### Getting ${CADENCE} ASAPP PII Data...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/asapp_pii_tables-${SCRIPT_VERSION}.hql \
 -hiveconf RUN_DATE="${RUN_DATE}" -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
 || { echo "HQL Failure"; exit 101; }

echo "### Getting ${CADENCE} ASAPP NOPII Data...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/asapp_nopii_tables-${SCRIPT_VERSION}.hql \
 -hiveconf RUN_DATE="${RUN_DATE}" -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
 || { echo "HQL Failure"; exit 101; }

if [ "$RUN_INIT" = "Yes" ]; then
   echo "### Creating Athena views ..."
   bash ./scripts/create_views-${SCRIPT_VERSION}.sh || { echo "HQL Failure"; exit 101;}
fi;
