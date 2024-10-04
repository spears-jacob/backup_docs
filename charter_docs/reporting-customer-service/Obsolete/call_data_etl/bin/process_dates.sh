#!/bin/bash

# get the overwrite date from the overwritedate table
export RUN_DATE="2018-10-26"


overwritedate="2018-10-19"

startdate=`date --date="$overwritedate -2 day" +%Y-%m-%d`
echo "Overwrite date: $overwritedate"

# get yesterdays date to limit the data from prod.asp_v_venona_events_portals
enddate="2018-10-26"

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
