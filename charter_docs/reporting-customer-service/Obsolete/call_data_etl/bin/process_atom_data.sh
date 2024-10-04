#!/bin/bash

#processdate=$(date --rfc-3339=date -d "-4 days")
#echo $processdate

# find the last partition date that has been inserted into staging to use as the process date
# processdate_to should be set to TODAY (run_date) if empty
processdate_to=$(hive -e "SELECT COALESCE(MAX(PARTITION_DATE),"1900-01-01") FROM \${env:ENVIRONMENT}.cs_atom_call_care_staging;")
echo "Last partition date entered: $processdate_to"

##processdate_from needs to be no earlier than 2018-10-20
if [ processdate_to == "1900-01-01" ]; then
  export processdate_from="2018-10-19"
else
  export processdate_from=`date --date="$processdate_to -$atom_process_buffer_days day" +%Y-%m-%d`
fi

echo "Processing from: $processdate_from to ${RUN_DATE}"

# insert data for each date in atom_call_care that isn't in staging yet.
for line in $(hive -e "SELECT DISTINCT PARTITION_DATE FROM prod.quantum_atom_call_care_v WHERE PARTITION_DATE > \"${processdate_from}\" ORDER BY PARTITION_DATE ASC;")
do

        echo "Processing partition_date=$line from prod.quantum_atom_call_care_v"
        hive -e "INSERT OVERWRITE TABLE \${env:ENVIRONMENT}.cs_atom_call_care_staging PARTITION (call_end_date_utc)
                  SELECT *, to_date(call_segment_stop_date_time_in_est)
                  FROM prod.quantum_atom_call_care_v
                  WHERE partition_date = \"${line}\";"

done

# get the minimum call_end_date in utc from atom_call_care, these are the partitions we want to update in cs_call_data
overwritedate=$(hive -e "SELECT MIN(to_date(to_utc_timestamp(substring(call_segment_stop_date_time_in_est,0,19),\"America/New_York\"))) FROM prod.quantum_atom_call_care_v WHERE PARTITION_DATE > \"${processdate_from}\";")

# Subtract one day from overwrite date because account data is in denver time.
# In queries > overwrite date is used for cs_atom_call_care_staging table and >= is used for all other tables.
export overwritedate=`date --date="$overwritedate -1 day" +%Y-%m-%d`

if [ overwritedate < "2018-10-19" ]; then
  export overwritedate="2018-10-19"
fi

# stick overwrite date into dev_tmp.cs_call_data_overwrite. Then we can query this value back out in the scala scripts as well as in the call in process.
hive -e 'DROP TABLE IF EXISTS ${env:TMP_db}.cs_call_data_overwrite;CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_call_data_overwrite AS SELECT '"'"${overwritedate}"'"' overwritedate;'
