#!/bin/bash

echo "### SB.net TWC ###"

echo "### Loading run date $RUN_DATE ###"

if [ "$RUN_DATE" != "" ]; then
  data_date=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`

  echo "### Looking for $data_date data ###"

  nifi_data_cnt=$(hive -e "SELECT COUNT(partition_hour) FROM $TMP_db.nifi_sbnet_twc_raw WHERE partition_date = '$RUN_DATE' AND $ENVIRONMENT.epoch_converter(post_cust_hit_time_gmt*1000,'America/New_York') = '$data_date';");
  nifi_data_cnt=(${nifi_data_cnt[@]})
  echo " --> Row count for $data_date: [${nifi_data_cnt[0]}]"

  if [ ${nifi_data_cnt[0]} -eq 0 ]; then
    echo "### ERROR: No data available for run date $RUN_DATE ($data_date data)" && exit 1
  fi

else
  echo "### ERROR: Run date value not available" && exit 1
fi
