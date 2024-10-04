#!/bin/bash

export extract_run_date="$RUN_DATE"

export START_DATE=`date --date="$extract_run_date - 7 day" +%Y-%m-%d`
export END_DATE=`date --date="$extract_run_date" +%Y-%m-%d`

export START_DATE_TZ=$(TZ=UTC date -d "$START_DATE `TZ="America/Denver" date -d "$START_DATE" +%Z`" +%Y-%m-%d_%H)
export END_DATE_TZ=$(TZ=UTC date -d "$END_DATE `TZ="America/Denver" date -d "$END_DATE" +%Z`" +%Y-%m-%d_%H)

echo '      {
        "START_DATE": "'"$START_DATE"'",
        "END_DATE":   "'"$END_DATE"'",
        "START_DATE_TZ": "'"$START_DATE_TZ"'",
        "END_DATE_TZ":   "'"$END_DATE_TZ"'"
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}" ;
