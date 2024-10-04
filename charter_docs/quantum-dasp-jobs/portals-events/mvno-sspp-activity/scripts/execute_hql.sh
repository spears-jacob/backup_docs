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
export export PROD_ACC_NO=387455165365
export ADDITIONAL_PARAMS=$7
#echo "Additional params: $ADDITIONAL_PARAMS"

export MVNO_TABLE=$(echo ${ADDITIONAL_PARAMS} | jq '.MVNO_TABLE' | tr -d '"' | sed "s/null//" )
#echo "MVNO_TABLE: $MVNO_TABLE"
export METRIC_TABLE=$(echo ${ADDITIONAL_PARAMS} | jq '.METRIC_TABLE' | tr -d '"' | sed "s/null//" )
#echo "METRIC_TABLE: $METRIC_TABLE"
export OUTPUT_TABLE=$(echo ${ADDITIONAL_PARAMS} | jq '.OUTPUT_TABLE' | tr -d '"' | sed "s/null//" )
#echo "OUTPUT_TABLE: $OUTPUT_TABLE"

if [ -z "$MVNO_TABLE" ]
	then
	if [ "$ENVIRONMENT" == "stg" ]; then
	    export MVNOTABLE=${PROD_ACC_NO}/prod_mob.mvno_accounts
	elif [ "$ENVIRONMENT" == "prod" ]; then
	    export MVNOTABLE=prod_mob.mvno_accounts
	fi
	echo "No mvno table passed, using default: $MVNOTABLE"
else
	export MVNOTABLE=$MVNO_TABLE
	echo "Using mvno table parameter: $MVNOTABLE"
fi

if [ -z "$METRIC_TABLE" ]
	then
	export METRICTABLE=$DASP_db.quantum_metric_agg_portals
	echo "No metric table passed, using default: $METRICTABLE"
else
	export METRICTABLE=$METRIC_TABLE
	echo "Using metric table parameter: $METRICTABLE"
fi

if [ -z "$OUTPUT_TABLE" ]
	then
	export OUTPUTTABLE=$DASP_db.mvno_sspp_activity
	echo "No output table passed, using default: $OUTPUTTABLE"
else
	export OUTPUTTABLE=$METRIC_TABLE
	echo "Using output table parameter: $OUTPUTTABLE"
fi

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
else
	VISIT_RANGE_END=`date -d "$RUN_DATE -3 day" +%Y-%m-%d` #3 Day lag as requested in original ticket
	START_DATE=`date -d "$VISIT_RANGE_END -31 day" +%Y-%m-%d` #Accounts that activated 31 days before that
	ACTIVATION_RANGE_END=`date -d "$VISIT_RANGE_END -30 day" +%Y-%m-%d` #since activation date is a datetime, we need a range


	echo "Running for "$RUN_DATE
	echo "Which means loading data for accounts activated on "$START_DATE
	echo "and includes possible visits up to "$VISIT_RANGE_END

  hive -S -v -f ${ARTIFACTS_PATH}/hql/mvno_sspp_activity-${SCRIPT_VERSION}.hql -hiveconf start_date=$START_DATE -hiveconf activation_range_end=$ACTIVATION_RANGE_END -hiveconf visit_range_end=$VISIT_RANGE_END -hiveconf mvno_table=$MVNOTABLE -hiveconf metric_table=$METRICTABLE -hiveconf output_table=$OUTPUTTABLE || { echo "HQL Failure"; exit 101; }
fi
