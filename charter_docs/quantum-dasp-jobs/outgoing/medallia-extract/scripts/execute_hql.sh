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
#export LAG_DAYS=4
#export LEAD_DAYS=3
export CADENCE=daily
export PROD_ACC_NO=387455165365
export S3_SECURE_SSPP="s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/medallia"


if [ "$ENVIRONMENT" == "stg" ]; then
    export PCQE=${PROD_ACC_NO}/prod.core_quantum_events_sspp
    export PMIS=${PROD_ACC_NO}/prod_dasp.asp_medallia_interceptsurvey
elif [ "$ENVIRONMENT" == "prod" ]; then
    export PCQE=prod.core_quantum_events_sspp
    export PMIS=prod_dasp.asp_medallia_interceptsurvey
fi

echo "### Pulling down UDF cipher keys..."
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo TIME_ZONE: $TZ
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;
echo STARTDATE60: $STARTDATE60;
FD=$(echo "${RUN_DATE//-}")
echo FD: ${FD}

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "
### Job Step Name echo:
  -> \${JOB_STEP_NAME} = ${JOB_STEP_NAME}
"

echo "
### Echoing S# Location: ${S3_SECURE_SSPP}
"

echo  "
### Echoing PWD variable: ${PWD}
"

if [ "${JOB_STEP_NAME}" == "1_medallia_extract" ]; then
  echo "### Running Medallia Extract ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/medallia_extract-${SCRIPT_VERSION}.hql -hiveconf START_DATE="${START_DATE}" || { echo "HQL Failure"; exit 101; }

elif [ "${JOB_STEP_NAME}" == "2_outgoing_medallia" ]; then
  export feed_purpose=survey_intercept
  export outfile="medallia_${feed_purpose}_${FD}.csv"
  echo "### Writing Outgoing Medallia ${feed_purpose} data ....................
  "
  hive -S -v -f ${ARTIFACTS_PATH}/hql/outgoing_secure_medallia-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
  mv 000000_0 ${outfile};
  # adds header
  sed -i -e '1i"application_name","acct_number","survey_action","biller_type","division","division_id","sys","prin","agent","acct_site_id","account_company","acct_franchise","day_diff","rpt_dt","partition_date_utc"' ${outfile};
  echo "
  ${outfile} has $(cat ${outfile} | wc -l) rows
  "
  ls -alh ${outfile} ;
  echo -e "\n### Moving Outgoing Medallia Intercept Survey data to secure location....................\n"
  aws s3 cp ${outfile} ${S3_SECURE_SSPP}/${feed_purpose}/${outfile}
  aws s3 ls ${S3_SECURE_SSPP}/${feed_purpose}/${outfile}
if [ $? -ne 0 ]; then echo -e "\n\tIt looks like the file ${outfile} did not transfer successfully"; kill -INT $$; fi
fi
