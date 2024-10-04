#!/bin/bash
# Run this just before you need the variables

export adt=$(date '+%Y-%m-%d_%H%M')

echo '{
        "YM": "'"$1"'",
        "IsReprocess": "'"$2"'",
        "CADENCE" : "'"$3"'",
        "ymd" : "'"$4"'",
        "adj" : "'"$5"'",
        "START_DATE" : "'"$6"'",
        "END_DATE" : "'"$7"'",
        "archive_date_time": "'"$adt"'"
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}"
