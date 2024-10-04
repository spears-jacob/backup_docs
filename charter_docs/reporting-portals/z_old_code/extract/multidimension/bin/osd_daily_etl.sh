#!/bin/bash
export extract_run_date=`date --date="$RUN_DATE" +%Y-%m-%d`
export TZ_OFFSET_DEN=$(TZ="America/Denver" date --date="$RUN_DATE" +%:::z | cut -d "-" -f2)

echo "
  ### Running Daily OS detail ETL for $extract_run_date
  ### Denver offset for this day is   $TZ_OFFSET_DEN
  "

export START_DATE_TZ=`date --date="$RUN_DATE -1 day" +%Y-%m-%d_$TZ_OFFSET_DEN`
export END_DATE_TZ=`date --date="$RUN_DATE" +%Y-%m-%d_$TZ_OFFSET_DEN`

echo "  ### START_DATE_TZ  $START_DATE_TZ"
echo "  ### END_DATE_TZ    $END_DATE_TZ"

echo "
  ### beginning execution
"
hive -f ../src/all_app_types/osd_01d_asp_multi_detailed_os.hql -hiveconf START_DATE_TZ=$START_DATE_TZ -hiveconf END_DATE_TZ=$END_DATE_TZ
