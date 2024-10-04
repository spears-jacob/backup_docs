#!/bin/bash
# This script uses the IsReprocess project.property to determine whether to process yesterday
# or reprocess a particular date (YYYY-MM-DD) found in the table specified in the project.properties
#
# In order to reprocess backwards one day at a time, schedule this job with IsReprocess set to 1
# with the cron minute set to 0/1 and ensure that the Concurrent Execution Option is set to
# Skip Execution: Do not run flow if it is already running.
# Keep in mind that this job will need to be manually unscheduled when the appropriate date range
# has been processed.

export tmp_rd=$TMP_db."$3"

if [ $1 -eq 0 ]; then
  export extract_run_date=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`

  echo "

  ### Running Daily Processing job for $extract_run_date

  "
fi

if [ $1 -eq 1 ]; then
  export extract_run_date=$(hive -e "SELECT * FROM $tmp_rd;");
  extract_before_date=`date --date="$extract_run_date -1 day" +%Y-%m-%d`
  echo "

  ### Running Processing job that goes backwards day by day for $extract_run_date

  "

fi

export START_DATE=$extract_run_date
export END_DATE=`date --date="$extract_run_date +1 day" +%Y-%m-%d`

export START_DATE_TZ=$(TZ=UTC date -d "$START_DATE `TZ="$2" date -d "$START_DATE" +%Z`" +%Y-%m-%d_%H)
export END_DATE_TZ=$(TZ=UTC date -d "$END_DATE `TZ="$2" date -d "$END_DATE" +%Z`" "+%Y-%m-%d %H")
#The job will also look at 3 hours into the next day to catch visits that started in process date
#but ended on next day
export END_DATE_TZ=`date --date="$END_DATE_TZ +3 hours" +%Y-%m-%d_%H`

echo '{
        "START_DATE": "'"$START_DATE"'",
        "END_DATE" : "'"$END_DATE"'",
        "START_DATE_TZ": "'"$START_DATE_TZ"'",
        "END_DATE_TZ" : "'"$END_DATE_TZ"'"
      }'

if [ $? -eq 0 ]; then
  echo "### SUCCESS"
  if [ $1 -eq 1 ]; then hive -e "INSERT OVERWRITE TABLE $tmp_rd VALUES('$extract_before_date');" ; fi
else
  echo "### ERROR: Job ended unsuccessfully -- Please re-run" && exit 1
fi
