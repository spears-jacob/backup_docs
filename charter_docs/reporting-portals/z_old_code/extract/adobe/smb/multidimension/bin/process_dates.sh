#!/bin/bash
# This script uses the IsReprocess project.property to determine whether to process yesterday
# or reprocess a particular date (YYY-MM-DD) found in the following table: $TMP_db.net_kpi_reprocess_run_date
# In order to reprocess, backwards one day at a time, keep running this job with IsReprocess set to 1.


tmp_rd=$TMP_db.asp_extract_adobe_smb_multidimension

if [ $# -ne 2 ]; then
    echo $0: Usage: reprocess.sh IsReprocess TZ
    exit 1
fi

if [ $1 -eq 0 ]; then
  export extract_run_date=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`

  echo "

  ### Running Daily Pre-Post processing job for $extract_run_date

  "
fi

if [ $1 -eq 1 ]; then
  export extract_run_date=$(hive -e "SELECT * FROM $tmp_rd;");
  extract_before_date=`date --date="$extract_run_date -1 day" +%Y-%m-%d`
  echo "

  ### Running Pre-Post reprocessing job that goes backwards day by day for $extract_run_date

  "

fi

export START_DATE=$extract_run_date
export END_DATE=`date --date="$extract_run_date +1 day" +%Y-%m-%d`

export START_DATE_TZ=$(TZ=UTC date -d "$START_DATE `TZ="$2" date -d "$START_DATE" +%Z`" +%Y-%m-%d_%H)
export END_DATE_TZ=$(TZ=UTC date -d "$END_DATE `TZ="$2" date -d "$END_DATE" +%Z`" +%Y-%m-%d_%H)


echo '{
        "START_DATE": "'"$START_DATE"'",
        "END_DATE" : "'"$END_DATE"'",
        "START_DATE_TZ": "'"$START_DATE_TZ"'",
        "END_DATE_TZ" : "'"$END_DATE_TZ"'"
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}"

if [ $? -eq 0 ]; then
  echo "### SUCCESS"
  if [ $1 -eq 1 ]; then hive -e "INSERT OVERWRITE TABLE $tmp_rd VALUES('$extract_before_date');" ; fi
else
  echo "### ERROR: Job ended unsuccessfully -- Please re-run" && exit 1
fi
