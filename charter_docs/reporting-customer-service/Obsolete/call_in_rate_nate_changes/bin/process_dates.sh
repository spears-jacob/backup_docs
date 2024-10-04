#!/bin/bash

# get the overwrite date from the overwritedate table

if [ $historical_load == 1 ]; then
  #overwritedate="2017-01-01"
  #startdate=$overwritedate
  hive -f ./init/historical_copy_dev_to_new.hql
  overwritedate=`date --date="$RUN_DATE -1 month" +%Y-%m-%d`
else
  overwritedate=$(hive -e "SELECT MAX(overwritedate) FROM \${env:TMP_db}.cs_call_data_overwrite;")
  # get two days prior to overwrite date to use as start for limiting the data from prod.asp_v_venona_events_portals
fi

startdate=`date --date="$overwritedate -2 day" +%Y-%m-%d`
echo "Overwrite date: $overwritedate"

# get yesterdays date to limit the data from prod.asp_v_venona_events_portals
enddate=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`

echo "Page Visits Date Range: $startdate - $enddate"

export overwritedate
export startdate
export enddate

echo '{
        "overwritedate": "'"$overwritedate"'",
        "startdate" : "'"$startdate"'",
        "enddate": "'"$enddate"'"
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}"
