#!/bin/bash

export extract_run_date="$RUN_DATE"

#Local
export START_DATE=`date --date="$extract_run_date - 2 day" +%Y-%m-%d`
export PART_DATE=`date --date="$extract_run_date - 1 day" +%Y-%m-%d`
export END_DATE=`date --date="$extract_run_date" +%Y-%m-%d`

#Time Zone
export START_DATE_TZ=$(TZ=UTC date -d "$START_DATE `TZ="America/Denver" date -d "$START_DATE" +%Z`" +%Y-%m-%d_%H)
export END_DATE_TZ=$(TZ=UTC date -d "$END_DATE `TZ="America/Denver" date -d "$END_DATE" +%Z`" +%Y-%m-%d_%H)

echo '      {
        "START_DATE": "'"$START_DATE"'",
        "PART_DATE": "'"$PART_DATE"'",
        "END_DATE":   "'"$END_DATE"'",
        "START_DATE_TZ": "'"$START_DATE_TZ"'",
        "PART_DATE_TZ": "'"$PART_DATE_TZ"'",
        "END_DATE_TZ":   "'"$END_DATE_TZ"'"
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}" ;
