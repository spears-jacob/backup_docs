#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_db=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export RUN_HOUR=$6

export ADDITIONAL_PARAMS=$7

### To reprocess multiple days, set the list of number of days to substract from RUN_DATE ("3 2 1"); send "0" to avoid reprocess step ###
export REPROCESS=$(echo ${ADDITIONAL_PARAMS} | jq '.REPROCESS' | tr -d '"' | sed "s/null//" )
if [ -z "${REPROCESS}" ]; then export REPROCESS="3 2 1"; fi

export DELETE_TABLE=$(echo ${ADDITIONAL_PARAMS} | jq '.DELETE_TABLE' | tr -d '"' | sed "s/null//" )
if [ -z "${DELETE_TABLE}" ]; then export DELETE_TABLE=""; fi

export DELETE_START_DATE_HOUR_UTC=$(echo ${ADDITIONAL_PARAMS} | jq '.DELETE_START_DATE_HOUR_UTC' | tr -d '"' | sed "s/null//" )
if [ -z "${DELETE_START_DATE_HOUR_UTC}" ]; then export DELETE_START_DATE_HOUR_UTC="0"; fi

export DELETE_END_DATE_HOUR_UTC=$(echo ${ADDITIONAL_PARAMS} | jq '.DELETE_END_DATE_HOUR_UTC' | tr -d '"' | sed "s/null//" )
if [ -z "${DELETE_END_DATE_HOUR_UTC}" ]; then export DELETE_END_DATE_HOUR_UTC="0"; fi

export SECURE_START_DATE_HOUR_UTC=$(echo ${ADDITIONAL_PARAMS} | jq '.SECURE_START_DATE_HOUR_UTC' | tr -d '"' | sed "s/null//" )
if [ -z "${SECURE_START_DATE_HOUR_UTC}" ]; then export SECURE_START_DATE_HOUR_UTC="0"; fi

export SECURE_END_DATE_HOUR_UTC=$(echo ${ADDITIONAL_PARAMS} | jq '.SECURE_END_DATE_HOUR_UTC' | tr -d '"' | sed "s/null//" )
if [ -z "${SECURE_END_DATE_HOUR_UTC}" ]; then export SECURE_END_DATE_HOUR_UTC="0"; fi

export TZ=America/Denver
export CADENCE=daily
export IsReprocess=0
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

execute_secure_pii_data(){
  echo DELETE_START_DATE_HOUR_UTC: $DELETE_START_DATE_HOUR_UTC
  echo DELETE_END_DATE_HOUR_UTC: $DELETE_END_DATE_HOUR_UTC
  echo SECURE_START_DATE_HOUR_UTC: $SECURE_START_DATE_HOUR_UTC
  echo SECURE_END_DATE_HOUR_UTC: $SECURE_END_DATE_HOUR_UTC

  echo "### Running ${CADENCE} Secure PII data (SI CQE table)...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/6_si_summary_secure_pii_data-${SCRIPT_VERSION}.hql \
   -hiveconf SECURE_START_DATE_HOUR_UTC="${SECURE_START_DATE_HOUR_UTC}" -hiveconf SECURE_END_DATE_HOUR_UTC="${SECURE_END_DATE_HOUR_UTC}" || { echo "HQL Failure"; exit 101; }
  
  source ./scripts/truncate_external_tables-${SCRIPT_VERSION}.sh $DELETE_TABLE $DELETE_START_DATE_HOUR_UTC $DELETE_END_DATE_HOUR_UTC
}

execute_reprocess_hql(){
	echo "### Date re-process: ${RUN_DATE}"
	source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsReprocess $TZ

	echo START_DATE: $START_DATE
	echo END_DATE: $END_DATE
	echo START_DATE_TZ: $START_DATE_TZ
	echo END_DATE_TZ: $END_DATE_TZ
	echo PRIOR_8DAYS_START_DATE: $PRIOR_8DAYS_START_DATE
	echo PRIOR_8DAYS_END_DATE: $PRIOR_8DAYS_END_DATE
	echo LABEL_DATE_DENVER: $LABEL_DATE_DENVER
	echo GRAIN: $GRAIN

	echo "### Running ${CADENCE} Self-Install Summary metrics (base table)...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/1_si_summary_page_base_master_daily_reprocess-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }
	 
	echo "### Running ${CADENCE} Self-Install Composite Score...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/3_si_composite_score_kpi_agg_daily-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }

	echo "### Running ${CADENCE} Self-Install Summary metrics (agg table)...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/2_si_summary_agg_master_daily-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
	 -hiveconf PRIOR_8DAYS_START_DATE="${PRIOR_8DAYS_START_DATE}" -hiveconf PRIOR_8DAYS_END_DATE="${PRIOR_8DAYS_END_DATE}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }
	 
	echo "### Running ${CADENCE} Self-Install Summary metrics (dummy records)...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/4_si_dummy_records_daily-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }
}

execute_hql(){
	echo "### Date process: ${RUN_DATE}"
	source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsReprocess $TZ
	
	echo START_DATE: $START_DATE
	echo END_DATE: $END_DATE
	echo START_DATE_TZ: $START_DATE_TZ
	echo END_DATE_TZ: $END_DATE_TZ
	echo PRIOR_8DAYS_START_DATE: $PRIOR_8DAYS_START_DATE
	echo PRIOR_8DAYS_END_DATE: $PRIOR_8DAYS_END_DATE
	echo LABEL_DATE_DENVER: $LABEL_DATE_DENVER
	echo GRAIN: $GRAIN

	echo "### Running ${CADENCE} Self-Install Summary metrics (base table)...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/1_si_summary_page_base_master_daily-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }

	echo "### Running ${CADENCE} Self-Install Composite Score...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/3_si_composite_score_kpi_agg_daily-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }
	
	echo "### Running ${CADENCE} Self-Install Summary metrics (agg table)...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/2_si_summary_agg_master_daily-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
	 -hiveconf PRIOR_8DAYS_START_DATE="${PRIOR_8DAYS_START_DATE}" -hiveconf PRIOR_8DAYS_END_DATE="${PRIOR_8DAYS_END_DATE}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }
	
	echo "### Running ${CADENCE} Self-Install Summary metrics (dummy records)...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/4_si_dummy_records_daily-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }
}

#Run script to secure pii data only if relevant parameters appear in $ADDITIONAL_PARAMS
if [ "${SECURE_START_DATE_HOUR_UTC}" != "0" ]; then
  execute_secure_pii_data
  exit 0 #Forcing exit to run execute_secure_pii_data function manually from Lambda avoiding to reload current date
fi

ORIGINAL_RUN_DATE=${RUN_DATE}

#Reprocess
if [ "${REPROCESS}" != "0" ]; then
	for i in ${REPROCESS}
	do
		export RUN_DATE=$(date -d "$ORIGINAL_RUN_DATE - $i day" +%F)
		execute_reprocess_hql
	done
fi

#Normal execution
export RUN_DATE=${ORIGINAL_RUN_DATE}
execute_hql

#Tableau source
echo "### Deleting partitions from si_summary_tableau to reload data based on START_DATE (90 days, 12 weeks, and 3 months)...................."
aws s3 rm s3://pi-qtm-si-${ENVIRONMENT}-aggregates-pii/data/${ENVIRONMENT}_dasp/si_summary_tableau/ --recursive

echo "### Running ${CADENCE} Self-Install Summary (Tableau source)...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/5_si_summary_tableau_daily-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }
