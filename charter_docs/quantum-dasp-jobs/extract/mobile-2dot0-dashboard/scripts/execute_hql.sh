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
export CADENCE=daily
export PROD_ACC_NO=387455165365

##if [ "$ENVIRONMENT" == "stg" ]; then
##    export M2MA=${PROD_ACC_NO}/prod_dasp.asp_m2dot0_metric_agg
##    export CPVC=${PROD_ACC_NO}/prod_dasp.cs_calls_with_prior_visits_prod_copy
##    export APLC=${PROD_ACC_NO}/prod_dasp.mvno_avgperlinecount_buckets_prod_copy
##    export MSAC=${PROD_ACC_NO}/prod_mob.mvno_smdh_accounts
if [ "$ENVIRONMENT" == "stg" ]; then
    export M2MA=stg_dasp.asp_m2dot0_metric_agg
    export CPVC=stg_dasp.cs_calls_with_prior_visits_prod_copy
    export APLC=stg_dasp.mvno_avgperlinecount_buckets_prod_copy
    export MSAC=${PROD_ACC_NO}/prod_mob.mvno_smdh_accounts_all
    export MSAH=${PROD_ACC_NO}/prod_mob.mvno_smdh_accounts_hist
    export PCQE=${PROD_ACC_NO}/prod.core_quantum_events_sspp
elif [ "$ENVIRONMENT" == "prod" ]; then
    export M2MA=prod_dasp.asp_m2dot0_metric_agg
    export CPVC=prod.cs_calls_with_prior_visits_prod
    export APLC=prod.mvno_avgperlinecount_buckets_prod
    export MSAC=prod_mob.mvno_smdh_accounts_all
    export MSAH=prod_mob.mvno_smdh_accounts_hist
    export PCQE=prod.core_quantum_events_sspp
fi

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo "### Adding supplementary custom dates"
export ENDDATE=$END_DATE
export STARTDATE_3day=`date --date="$END_DATE - 3 day" +%Y-%m-%d`
export STARTDATE_5day=`date --date="$END_DATE - 5 day" +%Y-%m-%d`
export STARTDATE_2week=`date --date="$END_DATE - 15 day" +%Y-%m-%d`
export STARTDATE_4week=`date --date="$END_DATE - 30 day" +%Y-%m-%d`
export VIEW_LAG_DATE=`date --date="$RUN_DATE - 5 day" +%Y-%m-%d`

echo TIME_ZONE: $TZ
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;
echo ENDDATE: $ENDDATE
echo STARTDATE_3day: $STARTDATE_3day
echo STARTDATE_5day: $STARTDATE_5day
echo STARTDATE_2week: $STARTDATE_2week
echo STARTDATE_4week: $STARTDATE_4week
echo VIEW_LAG_DATE: $VIEW_LAG_DATE

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

#echo "### Running Mobile 2.0 Dashboard ...................."
#hive -v -f ${ARTIFACTS_PATH}/hql/mobile_2dot0_dashboard-${SCRIPT_VERSION}.hql \
#-hiveconf START_DATE="${START_DATE}" || { echo "HQL Failure"; exit 101; }

#echo "Running View Create in Athena"
#bash ./scripts/create_views-${SCRIPT_VERSION}.sh || { echo "HQL Failure"; exit 101; }


if [ "${JOB_STEP_NAME}" == "1_mobile_2dot0_visitsranked" ]; then
  echo "### Running Mobile 2.0 visitsranked ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/mobile_2dot0_visitsranked-${SCRIPT_VERSION}.hql -hiveconf START_DATE="${START_DATE}" || { echo "HQL Failure"; exit 101; }

elif [ "${JOB_STEP_NAME}" == "2_mobile_2dot0_dashboard" ]; then
  echo "### Running Mobile 2.0 Dashboard ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/mobile_2dot0_dashboard-${SCRIPT_VERSION}.hql -hiveconf START_DATE="${START_DATE}" || { echo "HQL Failure"; exit 101; }

elif [ "${JOB_STEP_NAME}" == "3_m2dot0_api" ]; then
  echo "### Running Mobile 2.0 API Processing ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/mobile_2dot0_dashboard_api_processing-${SCRIPT_VERSION}.hql -hiveconf START_DATE="${START_DATE}" || { echo "HQL Failure"; exit 101; }

elif [ "${JOB_STEP_NAME}" == "4_view_create" ]; then
  echo "### Running Views Create ...................."
  bash ./scripts/create_views-${SCRIPT_VERSION}.sh || { echo "HQL Failure"; exit 101; }

elif [ "${JOB_STEP_NAME}" == "5_tableau_refresh" ]; then
  echo "### Running Tableau Dashboard Refresh ...................."
  source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
fi

# Getting cluster-id to write to intermediate bucket with cluster-id included in path
# https://stackoverflow.com/questions/20227091/does-an-emr-master-node-know-its-cluster-id
CLUSTER_ID=$(cat /mnt/var/lib/info/job-flow.json | jq -r ".jobFlowId") 
aws emr modify-cluster --cluster-id $CLUSTER_ID --step-concurrency-level 2 