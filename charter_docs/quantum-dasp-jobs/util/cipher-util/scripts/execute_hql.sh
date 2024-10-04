#!/bin/bash

echo "

######################################################################################################

#####     STEP 1:
#####     PRE-PROCESSING ENVIRONMENT AND JOB VARIABLES

######################################################################################################

"

echo -e "\n### EMR Utils - Load EMR Utils and Standard EMR environment (std_emr_env)"; mkdir -p scripts; hdfs dfs -get ${4}/scripts/*"${0: -11:-3}"*.*  ./scripts; source ./scripts/emr_utils-"${0: -11:-3}".sh; AP="${7:-none}";

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=$ENVIRONMENT
export ENV_DB=${ENVIRONMENT}_dasp
export SG_DB=${ENVIRONMENT}_sg
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export HQL_FILE=$5
export LKP_db=${ENVIRONMENT}_dasp
export HQL_SCRIPT_VERSION=$(echo $HQL_FILE | cut -d'-' -f 2 | cut -d '.' -f 1)
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"
export PROD_ACC_NO=387455165365

echo "

#####     CHECK OUT ENCRYPTION KEYS     #####

"
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "

#####     ARTIFACTS PROCESS - DOWNLOADING ARTIFACTS FROM S3 INTO SCRIPTS FOLDER     #####

"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

export ADDITIONAL_PARAMS=$7
export total_input_columns_count=$(echo ${ADDITIONAL_PARAMS} | jq '.total_input_columns_count' | tr -d '"' | sed "s/null//" )
export oper_input_columns_count=$(echo ${ADDITIONAL_PARAMS} | jq '.oper_input_columns_count' | tr -d '"' | sed "s/null//" )
export operation_type=$(echo ${ADDITIONAL_PARAMS} | jq '.operation_type' | tr -d '"' | sed "s/null//" )
export table_name=$(echo ${ADDITIONAL_PARAMS} | jq '.table_name' | tr -d '"' | sed "s/null//" )
export secure_input_location=$(echo ${ADDITIONAL_PARAMS} | jq '.secure_input_location' | tr -d '"' | sed "s/null//" )
export secure_output_location=$(echo ${ADDITIONAL_PARAMS} | jq '.secure_output_location' | tr -d '"' | sed "s/null//" )
export jiraticketnum=$(echo ${ADDITIONAL_PARAMS} | jq '.jiraticketnum' | tr -d '"' | sed "s/null//" )
export jiraticketnum_format=${jiraticketnum//-/$'_'}
export today=$(date +'%Y-%m-%d')
export today_format=$(date +'%Y_%m_%d')
export operationtype=${operation_type^^}
export tablename="$table_name""_$today_format"
export tablename_optype="$tablename""_$operationtype"
export tablename_optype_raw="$tablename_optype""_RAW"
export keep_columns=$[oper_input_columns_count + 1]
export operated_columns_count=$[total_input_columns_count + $oper_input_columns_count]
export post_operation_columns_count=$[oper_input_columns_count + $keep_columns]
export post_op_remainder_columns_count=$[total_input_columns_count - $oper_input_columns_count]
export unq_id=$(date +%s)
export athena_tablename="$table_name""_$operationtype""_$jiraticketnum_format"
export sec_output_location="$secure_output_location""$unq_id""/"

echo "

######################################################################################################

#####     STEP 2:
#####     S3 FILE PREPARATION AND VARIABLE BUILDING

######################################################################################################

"

export secure_input_location_current_run="$secure_input_location""$jiraticketnum_format"/
export secure_output_location_current_run="$secure_output_location""$jiraticketnum_format"/
printf -v input_filename '%s' `aws s3 ls $secure_input_location | awk '{print $4}'`
echo "$input_filename"
export secure_input_location_filename="$secure_input_location""$input_filename"
export secure_input_location_filename_current_run="$secure_input_location""$jiraticketnum_format"/"$input_filename"

echo "

#####     RUNNING: aws s3 cp $secure_input_location_filename $PWD/scripts/

"
aws s3 cp $secure_input_location_filename $PWD/scripts/
ls $PWD/scripts/

echo "

#####     MOVING $input_filename TO SECURE SUBFOLDER FOR CURRENT RUN $operationtype PROCESSING     #####

"

echo "

RUNNING: aws s3 cp $secure_input_location_filename $secure_input_location_current_run

"
aws s3 cp $secure_input_location_filename $secure_input_location_current_run

echo "

#####     REMOVING ORIGINAL $input_filename FROM $secure_input_location     #####

"
echo "

RUNNING: aws s3 rm $secure_input_location_filename

"
aws s3 rm $secure_input_location_filename

echo "

#####     VARIABLE PROCESSING: EXPORTING HEADER NAMES AND COLUMN NUMBERS     #####

"

export headers=`head -n 1 $PWD/scripts/$input_filename`
export names=(${headers// / })
export column_names_count=${#names[@]}
numbers_op=`
i=1
until [ $i -gt $oper_input_columns_count ]
do
  echo $i
  ((i=i+1))
done
`
numbers_end=`
i=$post_operation_columns_count
until [ $i -gt $operated_columns_count ]
do
  echo $i
  ((i=i+1))
done
`
export numbers_all=`echo "$numbers_op $numbers_end"`
export numbers_all="$numbers_op $numbers_end"
export nums=($numbers_all)
export column_names_count=${#names[@]}

echo "

#####     ECHOING ALL VARIABLES CREATED FOR JOB     #####

"

echo "ADDITIONAL_PARAMS is $ADDITIONAL_PARAMS"
echo "total_input_columns_count is $total_input_columns_count"
echo "oper_input_columns_count is $oper_input_columns_count"
echo "operation_type is $operation_type"
echo "operationtype is $operationtype"
echo "secure_input_location is $secure_input_location"
echo "secure_output_location is $secure_output_location"
echo "today is $today"
echo "today_format is $today_format"
echo "table_name is $table_name"
echo "tablename is $tablename"
echo "tablename_optype is $tablename_optype"
echo "tablename_optype_raw is $tablename_optype_raw"
echo "jiraticketnum is $jiraticketnum"
echo "jiraticketnum_format is $jiraticketnum_format"
echo "keep_columns is $keep_columns"
echo "operated_columns_count is $operated_columns_count"
echo "post_operation_columns_count is $post_operation_columns_count"
echo "post_op_remainder_columns_count is $post_op_remainder_columns_count"
echo "numbers_op is $numbers_op"
echo "numbers_end is $numbers_end"
echo "numbers_all is $numbers_all"
echo "column_names_count is $column_names_count"
echo "unq_id is $unq_id"
echo "athena_tablename is $athena_tablename"
echo "sec_output_location is $sec_output_location"
echo "Current working directory is $PWD"
echo "headers is $headers"
echo "nums is $nums"
echo "names is $names"
echo "column_names_count is $column_names_count"
echo "secure_input_location_current_run is $secure_input_location_current_run"
echo "secure_output_location_current_run is $secure_output_location_current_run"
echo "input_filename is $input_filename"
echo "secure_input_location_filename is $secure_input_location_filename"
echo "secure_input_location_filename_current_run is $secure_input_location_filename_current_run"

echo "

######################################################################################################

#####     STEP 3:
#####     CHECKING TO MAKE SURE EXPORTED VARIABLES ARE CORRECT

######################################################################################################

"

if ! [[ "$oper_input_columns_count" =~ ^[0-9]+$ ]]
    then
        echo " User Input invalid for oper_input_columns_count column. Only Integers are allowed "
        exit 1
fi
if ! [[ "$total_input_columns_count" =~ ^[0-9]+$ ]]
    then
        echo " User Input invalid for total_input_columns_count column. Only Integers are allowed "
        exit 1
fi
if  [[ "$operation_type" =~ 'encrypt' ]] && [[ "$operation_type" =~ 'decrypt' ]]
    then
        echo " User Input invalid. The only valid entries for operation_type are 'encrypt' or 'decrypt' "
        exit 1
fi
if  [[ -z "$table_name" ]]
    then
        echo " User Input invalid. User must include a table_name  "
        exit 1
fi
if  [[ -z "$secure_input_location" ]]
    then
        echo " User Input invalid. User must include a secure_input_location  "
        exit 1
fi
if  [[ -z "$secure_output_location" ]]
    then
        echo " User Input invalid. User must include a secure_output_location  "
        exit 1
fi
if  [[ -z "$today" ]]
    then
        echo " User Input invalid. User must include a today  "
        exit 1
fi
if  [[ "$column_names_count" != $total_input_columns_count ]]
    then
        echo " User Input invalid. The total number of values in the Columns names List must equal the total_input_columns_count variable "
        exit 1
fi

echo "

######################################################################################################

#####     STEP 4:
#####     TABLE AND QUERY BUILDING

######################################################################################################

"

#!/bin/bash
echo "

#####     BUILD TABLE TO DROP UNOPERATED DATA INTO     #####

"

echo "Creating table for raw data on which to operate"
echo "table_num_cols $total_input_columns_count"
create_stmt=` echo " create external table $ENV_DB.$tablename_optype("
for (( i=1; i<=$total_input_columns_count; i++ ))
do
echo "column$i string"
  if [[ $i != $total_input_columns_count ]]; then echo ","
  fi
done
echo ")row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'"
echo "with serdeproperties('separatorChar' = '\t','quoteChar' = '\"','serialization.encoding' = 'WINDOWS-1252')"
echo "LOCATION '$secure_input_location_current_run'"
echo "TBLPROPERTIES ('skip.header.line.count'='1')"
`
echo "exit for "
echo "create_stmt $create_stmt"

echo " dropping any existing table first"
hive -e "drop table if exists $ENV_DB.$tablename_optype"
echo " creating table with new file structure "
echo "ECHO OF HIVE CREATE hive -e "$create_stmt""
hive -e "$create_stmt"
if [ $? -eq 0 ]; then
      echo " create Query finished fine .........."
  else
     echo " ****FAILURE on hive create query *****"
      exit 1
  fi

echo "repairing the table after create"
hive -e "MSCK REPAIR TABLE $ENV_DB.$tablename_optype"

echo "

#####     BUILD QUERYABLE TABLE FOR POST-ENCRYPTED/DECRYPTED DATA     #####

"

echo "Creating table for data after $operationtype operation is performed"
echo "table_num_cols $operated_columns_count"
create_stmt_op=` echo " create external table $ENV_DB.$tablename_optype_raw("
for (( i=1; i<=$operated_columns_count; i++ ))
do
echo "column$i string"
  if [[ $i != $operated_columns_count ]]; then echo ","
  fi
done
echo ",distribution_date STRING"
echo ")row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'"
echo "with serdeproperties('separatorChar' = '\t','quoteChar' = '\"','serialization.encoding' = 'WINDOWS-1252')"
echo "LOCATION '$secure_output_location_current_run'"
echo "TBLPROPERTIES ('skip.header.line.count'='1')"
`
echo "exit for "
echo "create_stmt $create_stmt_op"

echo " dropping any existing table first"
hive -e "drop table if exists $ENV_DB.$tablename_optype_raw"
echo " creating table with new file structure "
echo "ECHO OF HIVE CREATE hive -e "$create_stmt_op""
hive -e "$create_stmt_op"
if [ $? -eq 0 ]; then
      echo " create Query finished fine .........."
  else
     echo " ****FAILURE on hive create query *****"
      exit 1
  fi

echo "

#####     BUILD SELECT OPERATION STATEMENT FROM THE TABLE CREATED FOR $operationtype OPERATION     #####

"

column_index=1 #operation columns start from 1 with additional params
operation_stmt=`for (( i=1; i<=$oper_input_columns_count; i++ ))
do
echo "ss_aes_${operationtype}256(column${column_index},'aes256')"
  if [[ $i != $oper_input_columns_count ]]; then echo ","
  fi
(( column_index=column_index + 1 ))
done `
echo "complete_select_stmt hive -e "SELECT *,$operation_stmt from $ENV_DB.$tablename_optype ""

echo "

#####     BUILD SELECT STATEMENT FOR ATHENA TABLE CREATE     #####

"

formatted_select=`
for index in ${!names[*]}; do 
  echo "column${nums[$index]} AS ${names[$index]}" 
  if [[ ${nums[$index]} != $operated_columns_count ]]; then echo ","
  fi
done
`
echo $formatted_select

echo "

######################################################################################################

#####     STEP 5:
#####     INSERT DATA INTO OUTPUT LOCATIONS

######################################################################################################

"

echo "

#####     RUNNING OPERATION TYPE: $operationtype ON INPUT DATA ....................     ####

"

echo "

#####     OUTPUTTING OPERATED DATA TO .TSV     #####

"

echo "

#####     Outputting operated data to RAW table     #####

"

echo "

#####     RUNNING: 

set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.auto.convert.join=false;
  CREATE TEMPORARY FUNCTION ss_aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256'
  USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar';
  CREATE TEMPORARY FUNCTION ss_aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256'
  USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar';
INSERT INTO $ENV_DB.$tablename_optype_raw SELECT * FROM (SELECT $operation_stmt,*, '$unq_id' AS distribution_date from $ENV_DB.$tablename_optype) sub DISTRIBUTE BY distribution_date;

"

hive -e "set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.auto.convert.join=false;
  CREATE TEMPORARY FUNCTION ss_aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256'
  USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar';
  CREATE TEMPORARY FUNCTION ss_aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256'
  USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar';
INSERT INTO $ENV_DB.$tablename_optype_raw SELECT * FROM (SELECT $operation_stmt,*, '$unq_id' AS distribution_date from $ENV_DB.$tablename_optype) sub DISTRIBUTE BY distribution_date;"

echo "

#####     Outputting operated data to TSV     #####

"

echo "

#####     RUNNING: 

set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.auto.convert.join=false;
set hive.cli.print.header=true;
  CREATE TEMPORARY FUNCTION ss_aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256'
  USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar';
  CREATE TEMPORARY FUNCTION ss_aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256'
  USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar';
SELECT $formatted_select FROM $ENV_DB.$tablename_optype_raw WHERE distribution_date = '$unq_id' > $tablename_optype.tsv

"

hive -e "set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.auto.convert.join=false;
set hive.cli.print.header=true;
  CREATE TEMPORARY FUNCTION ss_aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256'
  USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar';
  CREATE TEMPORARY FUNCTION ss_aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256'
  USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar';
SELECT $formatted_select FROM $ENV_DB.$tablename_optype_raw WHERE distribution_date = '$unq_id'" > $tablename_optype.tsv

echo "

#####     MOVING CLEANED OUTPUT DATA TO SECURE LOCATION     #####

"

aws s3 cp $tablename_optype.tsv $secure_output_location_current_run

if [[ "$operation_type" =~ 'encrypt' ]]; then
    echo "

#########################################################################################################

#####     STEP 6:
#####     ATHENA PROCESSING

#########################################################################################################

"

echo "

#####     RUNNING:

    execute_script_in_athena "DROP TABLE IF EXISTS $TMP_db.$athena_tablename PURGE"
    execute_script_in_athena "CREATE TABLE IF NOT EXISTS $TMP_db.$athena_tablename AS SELECT $formatted_select FROM $ENV_DB.$tablename_optype_raw WHERE distribution_date = '$unq_id'"

"

    execute_script_in_athena "DROP TABLE IF EXISTS $TMP_db.$athena_tablename PURGE"
    execute_script_in_athena "CREATE TABLE IF NOT EXISTS $TMP_db.$athena_tablename AS SELECT $formatted_select FROM $ENV_DB.$tablename_optype_raw WHERE distribution_date = '$unq_id'"

echo "

#########################################################################################################

#####     STEP 7:    
#####     CLEANUP OPERATIONS

#########################################################################################################

"
else
echo "

#########################################################################################################

#####     STEP 6:    
#####     CLEANUP OPERATIONS

#########################################################################################################

"
fi

echo "

RUNNING: aws s3 rm $secure_input_location_filename_current_run

"
aws s3 rm $secure_input_location_filename_current_run

echo "

RUNNING: hive -e drop table if exists $ENV_DB.$tablename_optype

"
hive -e "drop table if exists $ENV_DB.$tablename_optype"

echo "

RUNNING: hive -e drop table if exists $ENV_DB.$tablename_optype_raw

"
hive -e "drop table if exists $ENV_DB.$tablename_optype_raw"

echo "

#########################################################################################################
    
#####     $operationtype PROCESSING COMPLETE

#########################################################################################################

"