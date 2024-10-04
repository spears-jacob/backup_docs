#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_db=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export IDEN_db=${ENVIRONMENT}_iden

export TZ=America/Denver
export CADENCE="weekly"
export IsReprocess=0
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

execute_hql(){
	echo "### Date process..."
	source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsReprocess $TZ

	echo START_DATE: $START_DATE
	echo END_DATE: $END_DATE
	echo START_DATE_TZ: $START_DATE_TZ
	echo END_DATE_TZ: $END_DATE_TZ
	echo PRIOR_8DAYS_START_DATE: $PRIOR_8DAYS_START_DATE
	echo PRIOR_8DAYS_END_DATE: $PRIOR_8DAYS_END_DATE
	echo PRIOR_60DAYS_START_DATE: $PRIOR_60DAYS_START_DATE
	echo LABEL_DATE_DENVER: $LABEL_DATE_DENVER
	echo GRAIN: $GRAIN

	echo "### Running ${CADENCE} Self-Install activation channel metrics (agg table)...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/1_si_agg_activation_channel_weekly-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
	 -hiveconf PRIOR_8DAYS_START_DATE="${PRIOR_8DAYS_START_DATE}" -hiveconf PRIOR_8DAYS_END_DATE="${PRIOR_8DAYS_END_DATE}" \
	 -hiveconf PRIOR_60DAYS_START_DATE="${PRIOR_60DAYS_START_DATE}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }
	 
}


ORIGINAL_RUN_DATE=${RUN_DATE}

# ---------- Execution for 1 week ago based on RUN_DATE (Normal execution) ----------
# Since weekly query takes WOs from last week and call care table has a lag up to 3 days (in most of the cases), weekly job must be executed whole week
# to refresh call metrics within 8 days after WO date
REPROCESS_DAY=$(date --date=$ORIGINAL_RUN_DATE +%A)
MINUS_DAYS=-1

if [ "${REPROCESS_DAY}" = 'Sunday' ]; then
	#Normal execution
	MINUS_DAYS=0
elif [ "${REPROCESS_DAY}" = 'Monday' ]; then
	#Reprocess
	MINUS_DAYS=1
elif [ "${REPROCESS_DAY}" = 'Tuesday' ]; then 
	#Reprocess
	MINUS_DAYS=2
elif [ "${REPROCESS_DAY}" = 'Wednesday' ]; then
	#Reprocess
	MINUS_DAYS=3
elif [ "${REPROCESS_DAY}" = 'Thursday' ]; then 
	#Reprocess
	MINUS_DAYS=4
elif [ "${REPROCESS_DAY}" = 'Friday' ]; then 
	#Reprocess
	MINUS_DAYS=5
elif [ "${REPROCESS_DAY}" = 'Saturday' ]; then 
	#Reprocess
	MINUS_DAYS=6
fi

if [ ${MINUS_DAYS} -ge 0 ]; then
	echo "### Normal exectution..."
	export RUN_DATE=$(date -d "$ORIGINAL_RUN_DATE - $MINUS_DAYS day" +%F)
	execute_hql
fi
