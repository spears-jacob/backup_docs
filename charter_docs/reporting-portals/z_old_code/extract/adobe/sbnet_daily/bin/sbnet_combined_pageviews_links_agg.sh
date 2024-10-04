#!/bin/bash

echo "### Running SB.NET Combined PageViews/Links Daily Agg job for run_date of "$YESTERDAY_DEN

if [ "$YESTERDAY_DEN" != "" ]; then
  echo "##### Initiating sbnet_combined_pageviews_links_agg preprocess job ........."
  hive -f src/sbnet_daily_extracts_drop_partition.hql -hiveconf TABLE_NAME=sbnet_combined_pageviews_links_agg
  echo "##### Completed sbnet_combined_pageviews_links_agg preprocess job ........."

  echo "##### Starting SB.NET Combined PageViews/Links Daily Agg ........."
  hive -f src/sbnet_combined_pageviews_links_agg.hql
  if [ $? -eq 0 ]; then
    echo "### SUCCESS: SB.NET Combined PageViews/Links Daily Agg job finished"
  else
    echo "### ERROR: SB.NET Combined PageViews/Links Daily Agg ended unsuccessfully -- Please re-run" && exit 1
  fi
else
  echo "### ERROR: SB.NET Combined PageViews/Links Daily Agg job aborted due to a null run date" && exit 1
fi
