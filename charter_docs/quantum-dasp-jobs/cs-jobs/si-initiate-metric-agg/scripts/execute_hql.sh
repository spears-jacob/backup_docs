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
export LAG_DAYS=121
export LEAD_DAYS=120
export CADENCE=daily
export PROD_ACC_NO=387455165365


if [ "$ENVIRONMENT" == "stg" ]; then
    export SISA=stg_dasp.si_summary_agg_master
    export SI_AGG=${PROD_ACC_NO}/prod_dasp.si_page_agg
    export MA=${PROD_ACC_NO}/prod_dasp.quantum_metric_agg_portals
    export ISI_and_PI=stg_dasp.cs_initiate_si_and_pro_install_accounts
    export AAS=${PROD_ACC_NO}/prod.atom_accounts_snapshot
    export AWO=${PROD_ACC_NO}/prod.atom_work_orders
    export ASOI=${PROD_ACC_NO}/prod.atom_sales_order_insights
	export CC=${PROD_ACC_NO}/prod.atom_cs_call_care_data_3
elif [ "$ENVIRONMENT" == "prod" ]; then
    export SISA=prod_dasp.si_summary_agg_master
    export SI_AGG=prod_dasp.si_page_agg
    export MA=prod_dasp.quantum_metric_agg_portals
    export ISI_and_PI=prod_dasp.cs_initiate_si_and_pro_install_accounts
    export AAS=prod.atom_accounts_snapshot
    export AWO=prod.atom_work_orders
    export ASOI=prod.atom_sales_order_insights
	export CC=prod.atom_cs_call_care_data_3
fi

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

export STARTDATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 2 month + 1 day" +%F)
export ENDDATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 month + 1 day" +%F)
export STARTDATE_15=`date --date="$STARTDATE - 15 day" +%Y-%m-%d`
export ENDDATE_15=`date --date="$ENDDATE + 15 day" +%Y-%m-%d`

echo TIME_ZONE: $TZ
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;
echo LAST_MONTH_START_DATE: $LAST_MONTH_START_DATE;
echo LAST_MONTH_END_DATE: $LAST_MONTH_END_DATE;
echo LAST_MONTH_START_DATE_TZ: $LAST_MONTH_START_DATE_TZ;
echo LAST_MONTH_END_DATE_TZ: $LAST_MONTH_END_DATE_TZ;
echo LAST_MONTH_LABEL_DATE: $LAST_MONTH_LABEL_DATE;
echo STARTDATE: $STARTDATE;
echo ENDDATE: $ENDDATE;
echo STARTDATE_15: $STARTDATE_15;
echo ENDDATE_15: $ENDDATE_15;


export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
else

echo "### Running Self Install Digital Usage Metric Agg ...................."
hive -v -f ${ARTIFACTS_PATH}/hql/si_digital_usage_metric_agg-${SCRIPT_VERSION}.hql \
-hiveconf START_DATE="${START_DATE}" || { echo "HQL Failure"; exit 101; }

fi