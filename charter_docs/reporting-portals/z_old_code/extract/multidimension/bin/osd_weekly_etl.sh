#!/bin/bash
export extract_run_date=`date --date="$RUN_DATE" +%Y-%m-%d`
export TZ_OFFSET_DEN=$(TZ="America/Denver" date --date="$RUN_DATE" +%:::z | cut -d "-" -f2)

echo "
    ### Running Weekly OS detail ETL for $extract_run_date
  "

dayofweek=`date --date="$extract_run_date" +"%a"`
echo "    ### Day of week is $dayofweek"

if [ "$dayofweek" == "Thu" ];
then
  export START_DATE_TZ=`date --date="$RUN_DATE - 1 week" +%Y-%m-%d_$TZ_OFFSET_DEN`
  export END_DATE_TZ=`date --date="$RUN_DATE" +%Y-%m-%d_$TZ_OFFSET_DEN`
  echo "
    ### Beginning execution for week $START_DATE_TZ thru $END_DATE_TZ
    "
  hive -f ../src/all_app_types/osd_01w_asp_multi_detailed_os.hql -hiveconf START_DATE_TZ=$START_DATE_TZ -hiveconf END_DATE_TZ=$END_DATE_TZ
else
  echo "
    ### No weekly pull scheduled for $extract_run_date
  "
fi
