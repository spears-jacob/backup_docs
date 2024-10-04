#!/bin/bash
export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export PROD_ACC_NO=387455165365
export CADENCE=daily
export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

export unq_id=$(date +%s)
export s3_root="s3://pi-qtm-global-prod-sspp"
export s3_path_till_partitions="s3://pi-qtm-global-prod-sspp/data/prod/core_quantum_events_app_parts"
export local_tablename="$DASP_db.cqe_sspp_$unq_id"
export application_names_array=(SpecNet MySpectrum SMB PrivacyMicrosite IDManagement SpectrumCommunitySolutions)

if [ "$ENVIRONMENT" == "stg" ]; then
    export CQE_SSPP=$local_tablename
    export tablename_to_use_for_definition=${PROD_ACC_NO}/prod.core_quantum_events_sspp
elif [ "$ENVIRONMENT" == "prod" ]; then
    export CQE_SSPP=prod.core_quantum_events_sspp
    export tablename_to_use_for_definition=prod.core_quantum_events_sspp
fi

echo "### Pulling down UDF cipher keys..."
s3-dist-cp --src=$KEYS_S3_LOCATION --dest=hdfs:///enc_zone

echo "### Artifacts Process - Downloading artifacts from S3 into local folder"
hdfs dfs -get ${ARTIFACTS_PATH}/* .

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo "### Athena Utils - running script"
source ./scripts/athena_utils-${SCRIPT_VERSION}.sh

if [ "$ENVIRONMENT" == "stg" ]; then
  echo "### Running Athena Query in STG only to review portion of duplicated account guids"
  qry=$(cat ./hql/daily_check_account_guid_duplication-${SCRIPT_VERSION}.hql)
  execute_script_in_athena " $qry " || kill -INT $$;
  execute_script_in_athena " SELECT DISTINCT date_of_run, ct_dup_guids, ct_bck, proportion_duplicated FROM stg_dasp.asp_duplicate_account_guids_vs_bck ORDER BY date_of_run DESC " || kill -INT $$;
  get_athena_select_query_results
fi

echo "### Pull-down-portionof-table - running script (for a day)"
source ./scripts/pull_down_portion_of_table_to_local_hdfs-${SCRIPT_VERSION}.sh

#get create table definition
hive -S -e "show create table \`$tablename_to_use_for_definition\`;" > show_create_table_complete || { echo "HQL Failure"; exit 101; }

#clean up create table, remove first head line, remove backticks, revise table name to $local_tablename, and location to local hdfs
sed '/TBLPROPERTIES (/,$d' show_create_table_complete > show_create_table
#sed -i '1d' show_create_table
perl -pi -e 's/`//g' show_create_table
perl -pi -e "s/CREATE EXTERNAL TABLE.+\($/CREATE TABLE $local_tablename \(/g" show_create_table
perl -pi -e "s^$s3_path_till_partitions^/tmp/$local_tablename/^g" show_create_table

#run create table and repairs it
hive -S -v -f show_create_table || { echo "HQL Failure"; exit 101; }
hive -S -e "MSCK REPAIR TABLE $local_tablename;" || { echo "HQL Failure"; exit 101; }



echo -e "\n\n### Running Daily Adhoc job ...................."
echo START_DATE: $START_DATE
echo END_DATE: $END_DATE
echo START_DATE_TZ: $START_DATE_TZ
echo END_DATE_TZ: $END_DATE_TZ

hive -S -v -f ${ARTIFACTS_PATH}/hql/daily_adhoc_cqe_sspp-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }

execute_script_in_athena " DROP TABLE IF EXISTS $local_tablename;"
