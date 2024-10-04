#!/usr/bin/env bash
export RUN_DATE=$(date +%F)
export ENVIRONMENT=stg
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export PROD_ACC_NO=387455165365
export CADENCE=weekly

export unq_id=$(date +%s)
export s3_root="s3://pi-qtm-global-prod-sspp"
export s3_path_till_partitions="s3://pi-qtm-global-prod-sspp/data/prod/core_quantum_events_app_parts"
export tablename_to_use_for_definition=${PROD_ACC_NO}/prod.core_quantum_events_sspp
export local_tablename="cqe_sspp_$unq_id"
export application_names_array=(SpecNet MySpectrum SMB IDManagement PrivacyMicrosite SpectrumCommunitySolutions)

#folder housekeeping
export cwd=`pwd`
cd `mktemp -d`

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo "### Athena Utils - Download shell script"
aws s3 cp s3://pi-qtm-global-stg-artifacts/util/daily-adhoc/scripts/athena_utils-3add294b.sh athena_utils.sh

echo "### Athena Utils - Source shell script"
source athena_utils.sh

echo "### Pull Down a portion of table to local HDFS - Download shell script"
aws s3 cp s3://pi-qtm-global-stg-artifacts/util/daily-adhoc/scripts/pull_down_portion_of_table_to_local_hdfs-5f237345.sh pull_down_portion_of_table_to_local_hdfs.sh
source pull_down_portion_of_table_to_local_hdfs.sh

#get create table definition
hive -S -e "show create table \`$tablename_to_use_for_definition\`;" > show_create_table_complete || { echo "HQL Failure"; exit 101; }

#clean up create table, remove first head line, remove backticks, revise table name to $local_tablename, and location to local hdfs
sed '/TBLPROPERTIES (/,$d' show_create_table_complete > show_create_table
sed -i '1d' show_create_table
perl -pi -e 's/`//g' show_create_table
perl -pi -e "s/CREATE EXTERNAL TABLE.+\($/CREATE EXTERNAL TABLE $local_tablename \(/g" show_create_table
perl -pi -e "s^$s3_path_till_partitions^/tmp/$local_tablename/^g" show_create_table

#run create table and repairs it
hive -S -v -f show_create_table || { echo "HQL Failure"; exit 101; }
hive -S -e "MSCK REPAIR TABLE $local_tablename;" || { echo "HQL Failure"; exit 101; }
cd $cwd
unset cwd

echo -e "\n\n### Now Run the interactive job against the $local_tablename and do not forget to drop the table after using it...................."

selqry="SELECT count(domain) as Instances, SUM(IF(message__name = 'selectAction', 1, 0)) as select_actions, SUM(IF(message__name = 'pageView', 1, 0)) as page_views FROM $local_tablename; "

hive -S -e "$selqry;" || { echo "HQL Failure"; exit 101; }

echo $local_tablename
hadoop fs -du -h /tmp


### your commands here

execute_script_in_athena " DROP TABLE IF EXISTS $local_tablename;"

# to manually clean up the data from local hdfs, use the following commandd
 hdfs dfs -rm -R -skipTrash /tmp/$local_tablename/
