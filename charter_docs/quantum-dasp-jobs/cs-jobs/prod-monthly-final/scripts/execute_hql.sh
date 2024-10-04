#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

#############  Get unique IDs that will prevent tablename conflicts ############
export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
 EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"
export pwd=`pwd`
export STEP=${pwd#*-}
echo "
  EMR STEP ID: $STEP (not including the S- prefix)
"
############  Code starts here ############

RUNDATE=$(date -d "$RUN_DATE 3 days ago" +%Y-%m-%d) #we want to keep updating a fiscal momth's data until 3 days after that month ends
echo RUNDATE=$RUNDATE

STARTDATE=$(date -d "$RUN_DATE 40 days ago" +%Y-%m-%d) #Need to be sure our subquery includes all days of the fiscal month. 40 days ensures more than enough
echo STARTDATE=$STARTDATE

echo "Getting FISCALMONTH value with hive..."
FISCALMONTH=$(hive -e "SELECT fiscal_month FROM ${DASP_db}.chtr_fiscal_month WHERE partition_date='$RUNDATE';")
echo "Running for "$FISCALMONTH
# TODO need to get rid of hardcode
#FISCAL_MONTH=2020-06
# ^^^^ this is easy to do by using the fiscal_month date parameters in the universal process_dates.sh script (our standard)

hive -S -v -f ${ARTIFACTS_PATH}/hql/one_month_metrics-${SCRIPT_VERSION}.hql -hiveconf FISCAL_MONTH="${FISCALMONTH}" -hiveconf subquery_start=${STARTDATE} || { echo "HQL Failure"; exit 101; }

# checking query below -- not really at all anything to do with a view
hive -v -e "SELECT * FROM ${DASP_db}.cs_prod_monthly_fiscal_month_metrics WHERE fiscal_month = '${FISCALMONTH}';"
