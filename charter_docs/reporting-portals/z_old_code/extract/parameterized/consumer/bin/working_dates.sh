#!/bin/bash
# Run this just before you need the variables
# Unless reprocessing, set the RUN_DATE as needed to determine the date boundaries

#END DATES include the first day of the next month for TZ conversion from UTC to local
export LAST_MONTH_START_DATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 month + 1 day" +%F)
export LAST_MONTH_END_DATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days + 1 day" +%F)
export LAST_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$LAST_MONTH_START_DATE `TZ="$1" date -d "$LAST_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$LAST_MONTH_END_DATE `TZ="$1" date -d "$LAST_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_MONTH_START_DATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days + 1 day " +%F)
export CURRENT_MONTH_END_DATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days + 1 month + 1 day" +%F)
export CURRENT_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_MONTH_START_DATE `TZ="$1" date -d "$CURRENT_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_MONTH_END_DATE `TZ="$1" date -d "$CURRENT_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

export REPROCESS_MONTH_START_DATE=$(date -d "$4 -$(date -d $4 +%d) days + 1 day " +%F)
export REPROCESS_MONTH_END_DATE=$(date -d "$5 -$(date -d $5 +%d) days + 1 month + 1 day" +%F)
export REPROCESS_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$REPROCESS_MONTH_START_DATE `TZ="$1" date -d "$REPROCESS_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export REPROCESS_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$REPROCESS_MONTH_END_DATE `TZ="$1" date -d "$REPROCESS_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Fiscal END DATES include the first day of the next fiscal month for TZ conversion from UTC to local (21st is last day of fiscal month)
export LAST_FISCAL_MONTH_START_DATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 2 MONTH + 22 day" +%F)
export LAST_FISCAL_MONTH_END_DATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 MONTH + 22 day" +%F)
export LAST_FISCAL_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$LAST_FISCAL_MONTH_START_DATE `TZ="$1" date -d "$LAST_FISCAL_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_FISCAL_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$LAST_FISCAL_MONTH_END_DATE `TZ="$1" date -d "$LAST_FISCAL_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_FISCAL_MONTH_START_DATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 MONTH + 22 day " +%F)
export CURRENT_FISCAL_MONTH_END_DATE=$(date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days + 22 day" +%F)
export CURRENT_FISCAL_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FISCAL_MONTH_START_DATE `TZ="$1" date -d "$CURRENT_FISCAL_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_FISCAL_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FISCAL_MONTH_END_DATE `TZ="$1" date -d "$CURRENT_FISCAL_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

export REPROCESS_FISCAL_MONTH_START_DATE=$(date -d "$4 -$(date -d $4 +%d) days - 1 MONTH + 22 day " +%F)
export REPROCESS_FISCAL_MONTH_END_DATE=$(date -d "$5 -$(date -d $5 +%d) days + 22 day" +%F)
export REPROCESS_FISCAL_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$REPROCESS_FISCAL_MONTH_START_DATE `TZ="$1" date -d "$REPROCESS_FISCAL_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export REPROCESS_FISCAL_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$REPROCESS_FISCAL_MONTH_END_DATE `TZ="$1" date -d "$REPROCESS_FISCAL_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Daily END DATES include appropriate hours for TZ conversion from UTC to local
export PRIOR_DAY_START_DATE=$(date -d "$RUN_DATE - 1 day" +%F)
export PRIOR_DAY_END_DATE="$RUN_DATE"
export PRIOR_DAY_START_DATE_TZ=$(TZ=UTC date -d "$PRIOR_DAY_START_DATE `TZ="$1" date -d "$PRIOR_DAY_START_DATE" +%Z`" +%Y-%m-%d_%H)
export PRIOR_DAY_END_DATE_TZ=$(TZ=UTC date -d "$PRIOR_DAY_END_DATE `TZ="$1" date -d "$PRIOR_DAY_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_DAY_START_DATE="$RUN_DATE"
export CURRENT_DAY_END_DATE=$(date -d "$RUN_DATE + 1 day" +%F)
export CURRENT_DAY_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_DAY_START_DATE `TZ="$1" date -d "$CURRENT_DAY_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_DAY_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_DAY_END_DATE `TZ="$1" date -d "$CURRENT_DAY_END_DATE" +%Z`" +%Y-%m-%d_%H)

export REPROCESS_DAY_START_DATE="$4"
export REPROCESS_DAY_END_DATE=$(date -d "$5 + 1 day" +%F)
export REPROCESS_DAY_START_DATE_TZ=$(TZ=UTC date -d "$REPROCESS_DAY_START_DATE `TZ="$1" date -d "$REPROCESS_DAY_START_DATE" +%Z`" +%Y-%m-%d_%H)
export REPROCESS_DAY_END_DATE_TZ=$(TZ=UTC date -d "$REPROCESS_DAY_END_DATE `TZ="$1" date -d "$REPROCESS_DAY_END_DATE" +%Z`" +%Y-%m-%d_%H)


#Cadence below: Cadence_CalendarMonth1_FiscalMonth2_Daily3
if   [ $3 -eq 1 ]; then
  export CADENCE="monthly";
  export pf="year_month_Denver";
  export aplh="date_yearmonth(SUBSTRING(partition_date_hour_denver,0,10))"
  export apl="date_yearmonth(ne.partition_date)";
  export ape="(epoch_converter(cast(message__timestamp*1000 as bigint),\\\\\\\"$1\\\\\\\"))";
  export ap="date_yearmonth(epoch_converter(cast(message__timestamp*1000 as bigint),\\\\\\\"$1\\\\\\\"))";
  export adj="asp_net_monthly_agg_raw_adj"
  echo -e "\n\nNow processing using a cadence of CALENDAR MONTH.\n\n";

  if [ $2 -eq 1 ]; then
    export START_DATE_TZ="$CURRENT_MONTH_START_DATE_TZ" ;
    export END_DATE_TZ="$CURRENT_MONTH_END_DATE_TZ" ;
    export START_DATE="$CURRENT_MONTH_START_DATE" ;
    export END_DATE="$CURRENT_MONTH_END_DATE" ;
    export IsReprocess=0;
    echo -e "\n\nNow processing the CURRENT CALENDAR month.\n\n";
  elif [ $2 -eq 2 ]; then
    export START_DATE_TZ="$LAST_MONTH_START_DATE_TZ";
    export END_DATE_TZ="$LAST_MONTH_END_DATE_TZ" ;
    export START_DATE="$LAST_MONTH_START_DATE";
    export END_DATE="$LAST_MONTH_END_DATE" ;
    export IsReprocess=0;
    echo -e "\n\nNow processing the LAST CALENDAR month.\n\n";
  elif [ $2 -eq 3 ]; then
      export START_DATE_TZ="$REPROCESS_MONTH_START_DATE_TZ";
      export END_DATE_TZ="$REPROCESS_MONTH_END_DATE_TZ" ;
      export START_DATE="$REPROCESS_MONTH_START_DATE";
      export END_DATE="$REPROCESS_MONTH_END_DATE" ;
      export IsReprocess=1;
     echo -e "\n\nNow REPROCESSING certain CALENDAR month(s).\n\n";
  else
    echo 'UNDEFINED PROCESSING TIMEFRAME';
  fi ;
  export YM=${START_DATE:0:7};

elif [ $3 -eq 2 ]; then
  export CADENCE="fiscal_monthly";
  export pf="year_fiscal_month_Denver";
  export apl="fiscal_month";
  export aplh=$apl;
  export adj="asp_net_fiscal_monthly_agg_raw_adj"
  export ape="(epoch_converter(cast(message__timestamp*1000 as bigint),\\\\\\\"$1\\\\\\\"))";
  export ap="fiscal_month";
  echo -e "\n\nNow processing using a cadence of Charter FISCAL MONTH (22st prior to 21st current month) using $LKP_db.${fm_lkp}.\n\n";

  if [ $2 -eq 1 ]; then
    export START_DATE_TZ="$CURRENT_FISCAL_MONTH_START_DATE_TZ" ;
    export END_DATE_TZ="$CURRENT_FISCAL_MONTH_END_DATE_TZ" ;
    export START_DATE="$CURRENT_FISCAL_MONTH_START_DATE" ;
    export END_DATE="$CURRENT_FISCAL_MONTH_END_DATE" ;
    export IsReprocess=0;
    echo -e "\n\nNow processing the CURRENT FISCAL_MONTH.\n\n";
  elif [ $2 -eq 2 ]; then
    export START_DATE_TZ="$LAST_FISCAL_MONTH_START_DATE_TZ";
    export END_DATE_TZ="$LAST_FISCAL_MONTH_END_DATE_TZ" ;
    export START_DATE="$LAST_FISCAL_MONTH_START_DATE";
    export END_DATE="$LAST_FISCAL_MONTH_END_DATE" ;
    export IsReprocess=0;
    echo -e "\n\nNow processing the LAST FISCAL_MONTH.\n\n";
  elif [ $2 -eq 3 ]; then
      export START_DATE_TZ="$REPROCESS_FISCAL_MONTH_START_DATE_TZ";
      export END_DATE_TZ="$REPROCESS_FISCAL_MONTH_END_DATE_TZ" ;
      export START_DATE="$REPROCESS_FISCAL_MONTH_START_DATE";
      export END_DATE="$REPROCESS_FISCAL_MONTH_END_DATE" ;
      export IsReprocess=1;
     echo -e "\n\nNow REPROCESSING certain FISCAL MONTHS(s).\n\n";
  else
    echo 'UNDEFINED PROCESSING TIMEFRAME';
  fi ;
  export YM=${END_DATE:0:7};

elif [ $3 -eq 3 ]; then
  export CADENCE="daily";
  export pf="date_Denver";
  export aplh="SUBSTRING(partition_date_hour_denver,0,10)"
  export apl="ne.partition_date";
  export ape="(epoch_converter(cast(message__timestamp*1000 as bigint),\\\\\\\"$1\\\\\\\"))";
  export ap="$ape"
  export adj="asp_v_net_daily_agg_raw"
  echo -e "\n\nNow processing using a DAILY cadence.\n\n";

  if [ $2 -eq 1 ]; then
    export START_DATE_TZ="$CURRENT_DAY_START_DATE_TZ" ;
    export END_DATE_TZ="$CURRENT_DAY_END_DATE_TZ" ;
    export START_DATE="$CURRENT_DAY_START_DATE" ;
    export END_DATE="$CURRENT_DAY_END_DATE" ;
    export IsReprocess=0;
    echo -e "\n\nNow processing the CURRENT day.\n\n";
  elif [ $2 -eq 2 ]; then
    export START_DATE_TZ="$PRIOR_DAY_START_DATE_TZ";
    export END_DATE_TZ="$PRIOR_DAY_END_DATE_TZ" ;
    export START_DATE="$PRIOR_DAY_START_DATE";
    export END_DATE="$PRIOR_DAY_END_DATE" ;
    export IsReprocess=0;
    echo -e "\n\nNow processing the PRIOR DAY.\n\n";
  elif [ $2 -eq 3 ]; then
      export START_DATE_TZ="$REPROCESS_DAY_START_DATE_TZ";
      export END_DATE_TZ="$REPROCESS_DAY_END_DATE_TZ" ;
      export START_DATE="$REPROCESS_DAY_START_DATE";
      export END_DATE="$REPROCESS_DAY_END_DATE" ;
      export IsReprocess=1;
     echo -e "\n\nNow REPROCESSING certain day(s).\n\n";
  else
    echo 'UNDEFINED PROCESSING TIMEFRAME';
  fi ;
  export YM=$START_DATE;
else
  echo 'UNDEFINED CADENCE';
fi ;

export START_DATE_HOUR="$START_DATE"_00;
export END_DATE_HOUR="$END_DATE"_00;

echo '{
        "START_DATE": "'"$START_DATE"'",
        "END_DATE":   "'"$END_DATE"'",
        "START_DATE_TZ": "'"$START_DATE_TZ"'",
        "END_DATE_TZ":   "'"$END_DATE_TZ"'",
        "START_DATE_HOUR": "'"$START_DATE_HOUR"'",
        "END_DATE_HOUR":   "'"$END_DATE_HOUR"'",
        "CADENCE": "'"$CADENCE"'",
        "YM": "'"$YM"'",
        "PF": "'"$pf"'",
        "APL": "'"$apl"'",
        "APLH": "'"$aplh"'",
        "AP": "'"$ap"'",
        "APE": "'"$ape"'",
        "ADJ": "'"$adj"'",
        "IsReprocess": "'"$IsReprocess"'"
      }' > "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}"
