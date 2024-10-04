#!/bin/bash
export extract_run_date=`date --date="$RUN_DATE" +%Y-%m-%d`
export TZ_OFFSET_DEN=$(TZ="America/Denver" date --date="$RUN_DATE" +%:::z | cut -d "-" -f2)

echo "
    ### Running Monthly OS detail ETL for $extract_run_date
  "

dayofmonth=`date --date="$RUN_DATE" +%d`

if [ "$dayofmonth" == "01" ];
then
  export START_DATE_TZ=`date --date="$RUN_DATE - 1 month" +%Y-%m-01_$TZ_OFFSET_DEN`
  echo "START_DATE_TZ  $START_DATE_TZ"
  export END_DATE_TZ=`date --date="$RUN_DATE" +%Y-%m-%d_$TZ_OFFSET_DEN`
  echo "END_DATE_TZ    $END_DATE_TZ"

  echo "
    ### Beginning execution for month $START_DATE_TZ thru $END_DATE_TZ
  "
  hive -f ../src/all_app_types/osd_01m_asp_multi_detailed_os.hql -hiveconf START_DATE_TZ=$START_DATE_TZ -hiveconf END_DATE_TZ=$END_DATE_TZ
else
  echo "
    ### No monthly pull scheduled for $extract_run_date
  "
fi
