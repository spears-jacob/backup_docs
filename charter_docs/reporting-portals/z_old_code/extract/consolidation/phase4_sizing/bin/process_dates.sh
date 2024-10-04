#!/bin/bash
# This script uses the IsReprocess project.property to determine whether to process yesterday
# or reprocess a particular date (YYYY-MM-DD) found in the table specified in the project.properties
#
# In order to reprocess backwards one day at a time, schedule this job with IsReprocess set to 1
# with the cron minute set to 0/1 and ensure that the Concurrent Execution Option is set to
# Skip Execution: Do not run flow if it is already running.
# Keep in mind that this job will need to be manually unscheduled when the appropriate date range
# has been processed.


if [ $# -ne 3 ]; then
    echo $0: Usage: process_dates.sh IsReprocess TZ reprocess_date_table
    exit 1
fi

export tmp_rd=$TMP_db."$3"

if [ $1 -eq 0 ]; then
  export extract_run_date=`date --date="$RUN_DATE" +%Y-%m-%d`

  echo "

  ### Running Daily Processing job for $extract_run_date

  "
fi

if [ $1 -eq 1 ]; then
  export extract_run_date=$(hive -e "SELECT * FROM $tmp_rd;");
  extract_before_date=`date --date="$extract_run_date" +%Y-%m-%d`
  echo "

  ### Running Processing job that goes backwards day by day for $extract_run_date

  "

fi
run_first_date=$(date -d "$extract_run_date" +%Y%m01)
first_start=$(date -d "$run_first_date -1 month" +%Y-%m-%d)
export REPORT_MONTH=$(date -d "$run_first_date -1 month" +%Y-%m)
export ID_IDM_DATE=$(date -d "$run_first_date" +%Y-%m-%d)
export ACCOUNT_HISTORY_DATE=$(date -d "$run_first_date -1 day" +%Y-%m-%d)
export FID_START_DATE="${first_start}_00"
export FID_END_DATE="${ACCOUNT_HISTORY_DATE}_23"
export START_DATE="2017-01-01"
export END_DATE="2017-12-31"

max_idm_date=$(hive -e "SELECT upper(footprint),max(month) FROM prod.identities_idm_history group by upper(footprint);");
domain1=`echo $max_idm_date | cut -d' ' -f1`
value1=`echo $max_idm_date | cut -d' ' -f2`
domain2=`echo $max_idm_date | cut -d' ' -f3`
value2=`echo $max_idm_date | cut -d' ' -f4`
domain3=`echo $max_idm_date | cut -d' ' -f5`
value3=`echo $max_idm_date | cut -d' ' -f6`
declare MAX_$domain1=$value1
declare MAX_$domain2=$value2
declare MAX_$domain3=$value3
export MAX_IDM_TWC=$MAX_TWC
export MAX_IDM_BHN=$MAX_BHN
export MAX_IDM_CHARTER=$MAX_CHARTER

echo '{
        "ID_IDM_DATE": "'"$ID_IDM_DATE"'",
        "ACCOUNT_HISTORY_DATE" : "'"$ACCOUNT_HISTORY_DATE"'",
        "FID_START_DATE": "'"$FID_START_DATE"'",
        "FID_END_DATE" : "'"$FID_END_DATE"'",
        "START_DATE": "'"$START_DATE"'",
        "END_DATE" : "'"$END_DATE"'",
        "REPORT_MONTH" : "'"$REPORT_MONTH"'",
        "MAX_IDM_TWC" : "'"$MAX_IDM_TWC"'",
        "MAX_IDM_BHN" : "'"$MAX_IDM_BHN"'",
        "MAX_IDM_CHARTER" : "'"$MAX_IDM_CHARTER"'"
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}"

if [ $? -eq 0 ]; then
  echo "### SUCCESS"
  if [ $1 -eq 1 ]; then hive -e "INSERT OVERWRITE TABLE $tmp_rd VALUES('$extract_before_date');" ; fi
else
  echo "### ERROR: Job ended unsuccessfully -- Please re-run" && exit 1
fi
