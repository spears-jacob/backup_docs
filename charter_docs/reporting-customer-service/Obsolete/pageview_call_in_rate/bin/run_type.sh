#!/usr/bin/env bash


export START_DATE=$START_DATE
echo $START_DATE
export END_DATE=$END_DATE
echo $END_DATE

if [ $historical_load == 1 ]; then
  hive -f ../src/historical_pageview_call_in_rate.hql
else
  hive -f ../src/daily_pageview_call_in_rate.hql
fi
