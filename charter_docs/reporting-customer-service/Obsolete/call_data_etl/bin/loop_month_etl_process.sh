#!/bin/bash

for year in $(sh -c 'cd /data/tmp/ && find CALL_DATA*.txt | cut -c 11-14 | uniq')
do

  for month in $(sh -c 'cd /data/tmp/ && find CALL_DATA*$year*.txt | cut -c 15-16 | uniq')
  do

    echo "**Processing month $month-$year"
    hive -hiveconf processing_month=$month -hiveconf processing_year=$year -f ./src/etl_process.hql

  done

done
