#!/bin/bash

export extract_run_date="$RUN_DATE"
export TIMEZONE=America/Denver

#Calendar Monthly END DATES include the first day of the next month for TZ conversion from UTC to local
export START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 3 month + 1 day" +%F)
export END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 1 day" +%F)
export START_DATE_TZ=$(TZ=UTC date -d "$START_DATE `TZ="$TIMEZONE" date -d "$START_DATE" +%Z`" +%Y-%m-%d_%H)
export END_DATE_TZ=$(TZ=UTC date -d "$END_DATE `TZ="$TIMEZONE" date -d "$END_DATE" +%Z`" +%Y-%m-%d_%H)
export LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days " +%F)

echo '      {
        "START_DATE": "'"$START_DATE"'",
        "END_DATE":   "'"$END_DATE"'",
        "START_DATE_TZ": "'"$START_DATE_TZ"'",
        "END_DATE_TZ":   "'"$END_DATE_TZ"'",
        "LABEL_DATE": "'"$LABEL_DATE"'",
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}" ;
