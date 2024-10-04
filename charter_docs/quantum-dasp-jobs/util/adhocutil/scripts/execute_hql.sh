#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export RUN_HOUR=$6
export ADDITIONAL_PARAMS=$7
export FD=$(echo "${RUN_DATE//-}")

export   QUERY=$(echo ${ADDITIONAL_PARAMS} | jq '.QUERY'   | tr -d '"' | sed "s/null//" )
export WIGGLES=$(echo ${ADDITIONAL_PARAMS} | jq '.WIGGLES' | tr -d '"' | sed "s/null//" )
export WOGGLES=$(echo ${ADDITIONAL_PARAMS} | jq '.WOGGLES' | tr -d '"' | sed "s/null//" )

echo "### Displaying additional parameter inputs and expected variables after parsing
   ADDITIONAL_PARAMS = ${ADDITIONAL_PARAMS}

   QUERY    = $QUERY
   WIGGLES  = $WIGGLES
   WOGGLES  = $WOGGLES

"
export CADENCE=daily

echo -e "\n\n### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo -e "\n\n### FTP Process - Installing lftp"
sudo -s yum -y install lftp

echo -e "\n\n### FTP Process - Running sftp process"
bash ./scripts/lftp-${SCRIPT_VERSION}.sh ./scripts/example-${SCRIPT_VERSION}.json

echo -e "\n\n### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo -e "\n\n### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo -e "\n\n### Dates process - Process dates"
source process_dates.sh

echo TIME_ZONE: $TZ
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

export pwd=`pwd`
echo -e "\n\n### Execution is occurring the following directory:  $pwd\n\n"

export STEP=${pwd#*-}
echo "
  EMR STEP ID: $STEP (not including the S- prefix)
"

echo "
  Now displaying the credentials lengths to ensure they are being pulled.

"

# To find the name of the secret, search the gitlab CI/CD deploy step for "arn:aws:secretsmanager:us"
# then, the name is everything after "secret:" and not including the -#####, such as the following
# [id=arn:aws:secretsmanager:us-east-1:213705006773:secret:pi-qtm-dasp-stg-ancientftpcreds-H7dBQK]
# has a secret name of pi-qtm-dasp-stg-ancientftpcreds

SECRET_NAME=pi-qtm-dasp-${ENVIRONMENT}-ancientftpcreds
SECRET_STRING=$(aws secretsmanager get-secret-value --region us-east-1 --secret-id $SECRET_NAME | jq --raw-output '.SecretString')
user=$(echo $SECRET_STRING | jq -r '.["user"]')
pass=$(echo $SECRET_STRING | jq -r '.["pass"]')

echo \$SECRET_STRING is ${#SECRET_STRING} characters long
echo \$user is ${#user} characters long
echo \$pass is ${#pass} characters long

echo "
  Now the job runs

"

if [[ ${#QUERY} -eq 0 ]]; then
   export QUERY="SELECT 'Hello World, there was no query passed into this job step' "
fi

echo "
QRY: $QUERY

"

export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo -e "\n\n### Running Adhoc Utility Script (Run anything passed into the QRY variable, only using single quotes and no substitutions) ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/adhoc_util-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_TZ=${START_DATE_TZ} \
 -hiveconf END_DATE_TZ=${END_DATE_TZ} || { echo "HQL Failure"; exit 101; }

# testing below for secure buckets -- uncomment only when testing
# export acct=new_folder_goes_here_to_test
# export S3_SECURE_SSPP="s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/$acct"
# export fn="$acct_$CLUSTER_$STEP_${FD}.txt"
# echo "$acct - ${ENVIRONMENT} - example file here" > $fn
# aws s3 cp $fn $S3_SECURE_SSPP/$fn
# aws s3 ls $S3_SECURE_SSPP/$fn

