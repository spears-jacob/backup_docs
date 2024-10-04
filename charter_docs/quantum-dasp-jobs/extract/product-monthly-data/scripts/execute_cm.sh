#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export CADENCE=monthly

export SCALA_LIB=`ls /usr/lib/spark/jars/scala-library*`
echo "SCALA_LIB is ${SCALA_LIB}"

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
 EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"
export exec_id=$CLUSTER

export pwd=`pwd`
echo -e "\n\n### Execution is occurring the following directory:  $pwd\n\n"

# The step needs to be divided from the pwd directory using the below approach
# because the length of the step identifier varies.
export STEP=${pwd#*-}
echo "
  EMR STEP ID: $STEP (not including the S- prefix)
"

DAY=$(date -d "$RUN_DATE" '+%d')
if [ "$DAY" = "01" ]; then
  echo "$DAY is first day"

  echo "### Dates process - Process dates"
  source process_dates.sh

  echo TIME_ZONE: $TZ
  echo LABEL_DATE_DENVER=$LABEL_DATE_DENVER
  echo START_DATE=$START_DATE
  echo END_DATE=$END_DATE
  echo START_DATE_TZ=$START_DATE_TZ
  echo END_DATE_TZ=$END_DATE_TZ

  echo ".................... Running SpecMobile Login ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/specmobile_login-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE_TZ=${START_DATE_TZ} \
  -hiveconf END_DATE_TZ=${END_DATE_TZ} || { echo "HQL Failure"; exit 101; }
fi

#if [ "$DAY" = "09" -o "$DAY" = "10" -o "$DAY" = "11" ]; then
if [ "$DAY" != "29" -a "$DAY" != "30" -a "$DAY" != "31" -a "$DAY" != "01" -a "$DAY" != "02" ]; then
  export FIRST_DAY=$(echo $RUN_DATE | sed "s/..$/01/")
  export RUN_DATE=$FIRST_DAY

  echo "### Dates process - Process dates"
  source process_dates.sh

  echo TIME_ZONE: $TZ
  echo LABEL_DATE_DENVER=$LABEL_DATE_DENVER
  echo START_DATE=$START_DATE
  echo END_DATE=$END_DATE
  echo START_DATE_TZ=$START_DATE_TZ
  echo END_DATE_TZ=$END_DATE_TZ
  echo DASP_db=$DASP_db

  echo ".................... Running Digital Adoption Start...................."
  JOURNEY=Billing
  hive -S -v -f ${ARTIFACTS_PATH}/hql/digital_adoption_start-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE=${START_DATE} \
  -hiveconf END_DATE=${END_DATE} \
  -hiveconf JOURNEY=${JOURNEY} \
  -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" || { echo "HQL Failure"; exit 101; }

  echo ".................... Running Digital Adoption Journey...................."
  JOURNEYS="Change of Service,Enroll/Activate,Troubleshoot/Use,Other"
  IFS=","
  for JOURNEY in $JOURNEYS
  do
      echo "$JOURNEY"
      hive -S -v -f ${ARTIFACTS_PATH}/hql/digital_adoption_journey-${SCRIPT_VERSION}.hql \
      -hiveconf JOURNEY=${JOURNEY} \
      -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" || { echo "HQL Failure"; exit 101; }
  done

  echo ".................... Running Digital Adoption END...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/digital_adoption_end-${SCRIPT_VERSION}.hql \
  -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" || { echo "HQL Failure"; exit 101; }
fi
