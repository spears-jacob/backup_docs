#!/bin/bash
# Parameterized nifi new data check.  Please check that the project.property
# for nifi_raw has been set to reflect the appropriate table.  This also includes
# a Manual Operating Procedure in case data has not come through before the job began.

export nifi_raw=${1}
export nifi_lookup=${2}

if [ ${#nifi_raw} -eq 0 ]; then
  echo "
  ### ERROR: The nifi_raw project.property has not been set.
        This is the table where the raw nifi data is loaded.
       " && exit 1
fi

if [ ${#nifi_lookup} -eq 0 ]; then
  echo "
  ### ERROR: The nifi_lookup project.property has not been set.
        This is the table where the one of the nifi lookups data is loaded.
       " && exit 1
fi

echo "
  ### Loading run date $RUN_DATE ###
     "

if [ "$RUN_DATE" != "" ]; then
  data_date=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`

  echo "
  ### MSCK REPAIRING TABLE: $NIFI_db.$nifi_raw ###
  "
  hive -e "msck repair table $NIFI_db.$nifi_raw ;"

  echo "
  ### Looking for $data_date data ###
       "

  nifi_data_cnt=$(hive -e "SELECT COUNT(partition_hour) FROM $NIFI_db.$nifi_raw WHERE partition_date = '$RUN_DATE' AND $ENVIRONMENT.epoch_converter(post_cust_hit_time_gmt*1000,'America/New_York') = '$data_date';");
  nifi_data_cnt=(${nifi_data_cnt[@]})
  echo "

    --> Row count for $data_date in $NIFI_db.$nifi_raw: [${nifi_data_cnt[0]}]
       "

  max_lookup_partition_date=$(hive -e "SELECT MAX(partition_date) from $NIFI_db.$nifi_lookup;");

  # Check that date is valid, and keeps quiet unless there is an issue.
  date -d "$max_lookup_partition_date" >/dev/null
  if [ "$?" -ne 0 ] || [ ${#max_lookup_partition_date} -ne 10 ] ; then
    echo -e "\n\n### ERROR: Job ended unsuccessfully --\n\$max_lookup_partition_date: is not a valid date: $max_lookup_partition_date\nPlease see what is going on with the following: $NIFI_db.$nifi_lookup and try again.\n\n"
    exit 1
  fi

  echo '{
          "MX_PD": "'"$max_lookup_partition_date"'"
        }' > "${JOB_OUTPUT_PROP_FILE}"

  cat "${JOB_OUTPUT_PROP_FILE}"

  if [ ${nifi_data_cnt[0]} -eq 0 ]; then
    echo "
    ### ERROR: No data available for run date $RUN_DATE ($data_date data) in $NIFI_db.$nifi_raw

    MANUAL OPERATING PROCEDURE (MOP)
    In cases where data is not available at the time of execution (failed nifi_new_data_check
    job) but arrives later than anticipated, the whole job can be rerun once data is available.

    Check every hour or so to see if data has become available.  This is straightforward using
    the following query. It is based on the table queried in the nifi_new_data_check shell
    script, run by the job with the same name.

     RUN THE FOLLOWING ON A GV EDGE NODE TO CHECK FOR CURRENT DATA.

                      <=======================>

     hls /apps/hive/warehouse/$NIFI_db.db/$nifi_raw/partition_date=\$RUN_DATE

     export nifi_table=$NIFI_db.$nifi_raw

     hive -e \"SELECT COUNT (partition_hour)
               FROM \$nifi_table
               WHERE (partition_date='\$RUN_DATE')
               AND prod.epoch_converter(post_cust_hit_time_gmt*1000,'America/Denver') =
               (DATE_SUB('\$RUN_DATE', 1)); \"

                      <=======================>

     Once the above query returns results, then the job can be rerun in its entirety.


    " && exit 1
  fi

else
  echo "
  ### ERROR: Run date value not available
  " && exit 1
fi
