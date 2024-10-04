#!/bin/bash

net_view_last_count=`hive -e "SELECT * FROM $TMP_db.net_views_agg_last_date"`
net_view_last_timestamp=$net_view_last_count' 00:00:00'
if [ "$net_view_last_count" != "" ]; then
net_view_last_year=`date --date="$net_view_last_count" +%Y`
hive -f src/net_views_aggregate_overlap_exists.hql -hiveconf LAST_DATE=$net_view_last_count -hiveconf LAST_YEAR=$net_view_last_year
net_view_temp=`hive -e "SELECT COUNT(*) FROM $TMP_db.net_views_existing"`
if [ $net_view_temp -ne 0 ]; then
  echo "Overlap#####################"
  hive -f src/net_views_aggregate_overlap.hql -hiveconf LAST_DATE=$net_view_last_count -hiveconf LAST_YEAR=$net_view_last_year -hiveconf LAST_DATE_TIMESTAMP=$net_view_last_timestamp
  if [ $? -eq 0 ]; then
    echo "Overlap Process completed successfully#################"
    hive -f src/net_views_aggregate.hql -hiveconf LAST_DATE=$net_view_last_count -hiveconf LAST_YEAR=$net_view_last_year -hiveconf LAST_DATE_TIMESTAMP=$net_view_last_timestamp
    if [ $? -eq 0 ]; then
      echo "Aggregate Process completed successfully#################"
    else
      echo "Aggregate Process was not completed successfully####################"
      exit 1
    fi
  else
    echo "Overlap Process was not completed successfully####################"
    exit 1
  fi
else
  echo "No Overlap#######################"
  hive -f src/net_views_aggregate.hql -hiveconf LAST_DATE=$net_view_last_count -hiveconf LAST_YEAR=$net_view_last_year -hiveconf LAST_DATE_TIMESTAMP=$net_view_last_timestamp
  if [ $? -eq 0 ]; then
    echo "Aggregate Process completed successfully#################"
  else
    echo "Aggregate Process was not completed successfully####################"
    exit 1
  fi
fi
fi
