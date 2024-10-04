#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_db=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export RUN_HOUR=$6
export IDEN_db=${ENVIRONMENT}_iden
export ADDITIONAL_PARAMS=$7

### To reprocess multiple days, set the list of number of days to substract from RUN_DATE ("3 2 1"); send "0" to avoid reprocess step ###
export REPROCESS=$(echo ${ADDITIONAL_PARAMS} | jq '.REPROCESS' | tr -d '"' | sed "s/null//" )
if [ -z "REPROCESS" ]; then export REPROCESS="3 2 1"; fi

export TZ=America/Denver
export CADENCE=daily
export IsReprocess=0
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

echo "##### Copying encrypt UDF keys"
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "LEGACY_COMPANY: ${LEGACY_COMPANY}; AGG_INTERVAL: ${AGG_INTERVAL}; LINEAR_FILE_TYPE: ${LINEAR_FILE_TYPE}"

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"


hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

execute_hql(){
	echo "### Date process: ${RUN_DATE}"
	source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsReprocess $TZ
	
	echo START_DATE: $START_DATE
	echo END_DATE: $END_DATE
	echo START_DATE_TZ: $START_DATE_TZ
	echo END_DATE_TZ: $END_DATE_TZ
	echo PRIOR_8DAYS_START_DATE: $PRIOR_8DAYS_START_DATE
	echo PRIOR_8DAYS_END_DATE: $PRIOR_8DAYS_END_DATE
	echo PRIOR_15DAYS_START_DATE: $PRIOR_15DAYS_START_DATE
	echo LABEL_DATE_DENVER: $LABEL_DATE_DENVER
	echo GRAIN: $GRAIN

	echo "### Running ${CADENCE} Self-Install etos data processing...................."
	hive -S -v -f ${ARTIFACTS_PATH}/hql/1_si_etos_collateral_daily-${SCRIPT_VERSION}.hql \
	 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
	 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
	 -hiveconf PRIOR_15DAYS_START_DATE="${PRIOR_15DAYS_START_DATE}" \
	 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
	 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
	 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }

}


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
