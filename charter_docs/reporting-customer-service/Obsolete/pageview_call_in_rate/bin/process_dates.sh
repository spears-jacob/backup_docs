#!/bin/bash
# This script extracts yesterdays date to use to run in the hive queries
# If project.properties->historical_load=1 then process all history
# If project.properties->historical_load<>1 then only process yesterdays data

if [ $historical_load == 1 ]; then
  extract_run_date="2018-08-24"
else
  extract_run_date=$(hive -e "SELECT COALESCE(to_date(MAX(partition_date_utc)),to_date(\"2018-08-24\")) FROM \${env:ENVIRONMENT}.cs_venona_events;")
  extract_run_date=`date --date="$extract_run_date -5 day" +%Y-%m-%d`
fi

export START_DATE=$extract_run_date
export END_DATE=`date --date="$RUN_DATE +1 day" +%Y-%m-%d`

echo "

### Running Daily Processing job for $extract_run_date

"

echo '{
        "START_DATE": "'"$START_DATE"'",
        "END_DATE" : "'"$END_DATE"'"
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}"
