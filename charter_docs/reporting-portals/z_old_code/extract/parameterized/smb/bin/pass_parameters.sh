#!/bin/bash
# Run this just before you need the variables

export adt=$(date '+%Y-%m-%d_%H%M')

echo '{
        "YM": "'"$1"'",
        "IsReprocess": "'"$2"'",
        "CADENCE" : "'"$3"'",
        "PF" : "'"$4"'",
        "ADJ" : "'"$5"'",
        "START_DATE_TZ" : "'"$6"'",
        "END_DATE_TZ" : "'"$7"'",
        "AP" : "'"$8"'",
        "APE" : "'"$9"'",
        "archive_date_time": "'"$adt"'"
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}"
