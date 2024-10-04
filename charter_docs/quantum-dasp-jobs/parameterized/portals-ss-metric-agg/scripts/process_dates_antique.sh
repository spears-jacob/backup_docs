#!/bin/bash
# This script uses the IsReprocess project.property to determine whether to process yesterday
# or reprocess a particular date (YYYY-MM-DD) found in the table specified in the project.properties
#
# FUTURE ITEMS FOR CONSIDERATION (KKellner's ideas)
# 1. For any job that will reprocess more than 100 days, alert the OPS team that the reprocess will take some time.
# 2. Centrally log reprocessing status so anyone can note progress for a given job
# 3. Abstract this reprocess construct for use on any HQL / flow
# 4. Tie the aforementioned abstracted construct with a web user interface (dropdown
#    menus) to allow for universal access to both the initiation and logging of reprocess jobs.
#
# Also, note that cadence is set in the evironment that process_dates.sh is called.
# CADENCE = [daily, fiscal_monthly, monthly, weekly]

# Set ShowReprocessCalculationOutput to 1 to see calculation output in the log
export ShowReprocessCalculationOutput=1;
export FLOW_BACKFILL="backfill";

if [ $# -lt 1 ]; then
    echo $0: Usage: process_dates.sh IsReprocess TZ [reprocess_date_table] [reprocess_earliest_date_label]
    exit 1
fi

export IsReprocess="$1"
export GRAIN="$CADENCE"

if [ $IsReprocess -eq 0 ]; then
  export extract_run_date=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`

  echo "

  ### Running the typical $CADENCE Processing job for $extract_run_date

  "
fi

if [ $IsReprocess -eq 1 ]; then
  export tmp_rd=$TMP_db."$3"
  export extract_run_date=$(hive -e "SELECT * FROM $tmp_rd;");
  export earliest_date_label="$4"

  # Check that dates are valid, and keeps quiet unless there is an issue.
  date -d "$earliest_date_label" >/dev/null
  if [ "$?" -ne 0 ] || [ ${#earliest_date_label} -ne 10 ] ; then
    echo -e "\n\n### ERROR: Job ended unsuccessfully --\n\$earliest_date_label: is not a valid date: $earliest_date_label\nPlease revise ReprocessingEarliestDateLabel to a valid date in the project.properties and try again.\n\n"
    exit 1
  fi

  date -d "$extract_run_date" >/dev/null
  if [ "$?" -ne 0 ] || [ ${#extract_run_date} -ne 10 ] ; then
    echo -e "\n\n### ERROR: Job ended unsuccessfully --\n\$extract_run_date: is not a valid date: $extract_run_date\nPlease revise the date in $tmp_rd and try again.\n\n"
    exit 1
  fi
fi

#Calendar Monthly END DATES include the first day of the next month for TZ conversion from UTC to local
export LAST_MONTH_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 month + 1 day" +%F)
export LAST_MONTH_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 1 day" +%F)
export LAST_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$LAST_MONTH_START_DATE `TZ="$2" date -d "$LAST_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$LAST_MONTH_END_DATE `TZ="$2" date -d "$LAST_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_MONTH_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days " +%F)


export CURRENT_MONTH_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 1 day " +%F)
export CURRENT_MONTH_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 1 month + 1 day" +%F)
export CURRENT_MONTH_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 1 month" +%F)

export CURRENT_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_MONTH_START_DATE `TZ="$2" date -d "$CURRENT_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_MONTH_END_DATE `TZ="$2" date -d "$CURRENT_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Fiscal Monthly END DATES include the first day of the next fiscal month for TZ conversion from UTC to local (21st is last day of fiscal month)
export LAST_FISCAL_MONTH_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 2 MONTH + 22 day" +%F)
export LAST_FISCAL_MONTH_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 22 day" +%F)
export LAST_FISCAL_MONTH_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 21 day" +%F)
export LAST_FISCAL_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$LAST_FISCAL_MONTH_START_DATE `TZ="$2" date -d "$LAST_FISCAL_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_FISCAL_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$LAST_FISCAL_MONTH_END_DATE `TZ="$2" date -d "$LAST_FISCAL_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_FISCAL_MONTH_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 22 day " +%F)
export CURRENT_FISCAL_MONTH_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 22 day" +%F)
export CURRENT_FISCAL_MONTH_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 21 day" +%F)
export CURRENT_FISCAL_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FISCAL_MONTH_START_DATE `TZ="$2" date -d "$CURRENT_FISCAL_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_FISCAL_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FISCAL_MONTH_END_DATE `TZ="$2" date -d "$CURRENT_FISCAL_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Daily END DATES include appropriate hours for TZ conversion from UTC to local
export PRIOR_DAY_START_DATE=$(date -d "$extract_run_date - 1 day" +%F)
export PRIOR_DAY_END_DATE="$extract_run_date"
export PRIOR_DAY_START_DATE_TZ=$(TZ=UTC date -d "$PRIOR_DAY_START_DATE `TZ="$2" date -d "$PRIOR_DAY_START_DATE" +%Z`" +%Y-%m-%d_%H)
export PRIOR_DAY_END_DATE_TZ=$(TZ=UTC date -d "$PRIOR_DAY_END_DATE `TZ="$2" date -d "$PRIOR_DAY_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_DAY_START_DATE="$extract_run_date"
export CURRENT_DAY_END_DATE=$(date -d "$extract_run_date + 1 day" +%F)
export CURRENT_DAY_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_DAY_START_DATE `TZ="$2" date -d "$CURRENT_DAY_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_DAY_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_DAY_END_DATE `TZ="$2" date -d "$CURRENT_DAY_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Weekly END Dates are a running-7-day lag, including appropriate hours for TZ conversion from UTC to local
export CURRENT_WEEK_START_DATE=`date --date="$extract_run_date - 6 day" +%Y-%m-%d`
export CURRENT_WEEK_END_DATE=`date --date="$extract_run_date + 1 day" +%Y-%m-%d`
export LABEL_DATE_DENVER="$extract_run_date"

export CURRENT_WEEK_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_WEEK_START_DATE `TZ="$2" date -d "$CURRENT_WEEK_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_WEEK_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_WEEK_END_DATE `TZ="$2" date -d "$CURRENT_WEEK_END_DATE" +%Z`" +%Y-%m-%d_%H)

export processing_started_date_time_denver=$(TZ=America/Denver date -d @$(date -u +%s) "+%Y-%m-%d %H:%M:%S")
export processing_started_by_user=$(id -un)

# Reprocessing considerations below
export ContinueReprocess=0;
if [ $IsReprocess -eq 1 ]; then

  if   [ $CADENCE == "monthly" ]; then
    export extract_before_date="$LAST_MONTH_LABEL_DATE";
  elif [ $CADENCE == "fiscal_monthly" ]; then
    export extract_before_date="$LAST_FISCAL_MONTH_LABEL_DATE";
  elif [ $CADENCE == "daily" ]; then
    export extract_before_date="$PRIOR_DAY_START_DATE";
  elif [ $CADENCE == "weekly" ]; then
    export extract_before_date="$PRIOR_DAY_START_DATE";
  fi


  export extract_before_date_epoch_seconds=$(date --date="$extract_before_date" +%s)
  export earliest_date_label_epoch_seconds=$(date --date="$earliest_date_label" +%s)

  if [ $extract_before_date_epoch_seconds -ge $earliest_date_label_epoch_seconds ]; then
    export ContinueReprocess=1;
  else
    export ContinueReprocess=0;
  fi

  if [ $ShowReprocessCalculationOutput -eq 1 ]; then
    echo -e "\n\n\tprocess started at: $processing_started_date_time_denver\n\tprocess started by: $processing_started_by_user\n\n"

    echo -e "\t\$extract_before_date: $extract_before_date\n\t\$earliest_date_label: $earliest_date_label\n\n\t\$extract_before_date_epoch_seconds: $extract_before_date_epoch_seconds\n\t\$earliest_date_label_epoch_seconds: $earliest_date_label_epoch_seconds\n"

    echo -e "\n\t\$ContinueReprocess:\t$ContinueReprocess\n"
  fi
fi


#Cadence below, from externally exported CADENCE environment variable
if   [ $CADENCE == "monthly" ]; then
  export START_DATE_TZ="$CURRENT_MONTH_START_DATE_TZ" ;
  export END_DATE_TZ="$CURRENT_MONTH_END_DATE_TZ" ;
  export START_DATE="$CURRENT_MONTH_START_DATE" ;
  export END_DATE="$CURRENT_MONTH_END_DATE" ;
  export LABEL_DATE_DENVER="$CURRENT_MONTH_LABEL_DATE";

  if [ $IsReprocess -eq 0 ]; then
    echo -e "\n\nNow processing the CURRENT CALENDAR month ending → $CURRENT_MONTH_LABEL_DATE\n\n";
  else
    echo -e "\n\nNow reprocessing the CURRENT CALENDAR month ending → $CURRENT_MONTH_LABEL_DATE\n\n"
    if [ $ContinueReprocess -eq 1 ]; then echo -e "The next month to be processed ends $extract_before_date\n\n"; fi
  fi ;

elif [ $CADENCE == "fiscal_monthly" ]; then
  export START_DATE_TZ="$CURRENT_FISCAL_MONTH_START_DATE_TZ" ;
  export END_DATE_TZ="$CURRENT_FISCAL_MONTH_END_DATE_TZ" ;
  export START_DATE="$CURRENT_FISCAL_MONTH_START_DATE" ;
  export END_DATE="$CURRENT_FISCAL_MONTH_END_DATE" ;
  export LABEL_DATE_DENVER="$CURRENT_FISCAL_MONTH_LABEL_DATE"

  if [ $IsReprocess -eq 0 ]; then
    echo -e "\n\nNow processing the FISCAL_MONTH ending → $CURRENT_FISCAL_MONTH_LABEL_DATE\n\n";
  else
    echo -e "\n\nNow reprocessing the CURRENT FISCAL month → $CURRENT_FISCAL_MONTH_LABEL_DATE\n\n"
    if [ $ContinueReprocess -eq 1 ]; then echo -e "The next fiscal month to be processed ends $extract_before_date\n\n"; fi
  fi ;

elif [ $CADENCE == "daily" ]; then
  export START_DATE_TZ="$CURRENT_DAY_START_DATE_TZ" ;
  export END_DATE_TZ="$CURRENT_DAY_END_DATE_TZ" ;
  export START_DATE="$CURRENT_DAY_START_DATE" ;
  export END_DATE="$CURRENT_DAY_END_DATE" ;
  export LABEL_DATE_DENVER="$START_DATE"

  if [ $IsReprocess -eq 0 ]; then
    echo -e "\n\nNow processing the PRIOR day → $extract_run_date\n\n";
  else
    echo -e "\n\nNow reprocessing the CURRENT day → $START_DATE\n\n"
    if [ $ContinueReprocess -eq 1 ]; then echo -e "The next day to be processed is $extract_before_date\n\n"; fi
  fi ;
elif [ $CADENCE == "weekly" ]; then
  export START_DATE_TZ="$CURRENT_WEEK_START_DATE_TZ" ;
  export END_DATE_TZ="$CURRENT_WEEK_END_DATE_TZ" ;
  export START_DATE="$CURRENT_WEEK_START_DATE" ;
  export END_DATE="$CURRENT_WEEK_END_DATE" ;

  if [ $IsReprocess -eq 0 ]; then
    echo -e "\n\nNow processing the WEEK ending on the PRIOR day → $extract_run_date\n\n";
  else
      echo -e "\n\nNow reprocessing the WEEK ending on the CURRENT day → $LABEL_DATE_DENVER\n\n"
    if [ $ContinueReprocess -eq 1 ]; then echo -e "The next Week ending day to be processed ends $extract_before_date\n\n"; fi
  fi ;
else
  echo "
  ### ERROR: UNDEFINED CADENCE -- Please re-run
  " && exit 1
fi ;

echo '      {
        "START_DATE": "'"$START_DATE"'",
        "END_DATE":   "'"$END_DATE"'",
        "START_DATE_TZ": "'"$START_DATE_TZ"'",
        "END_DATE_TZ":   "'"$END_DATE_TZ"'",
        "GRAIN": "'"$GRAIN"'",
        "LABEL_DATE_DENVER": "'"$LABEL_DATE_DENVER"'",
        "FLOW_BACKFILL" : "'"$FLOW_BACKFILL"'",
        "IsReprocess": "'"$IsReprocess"'",
        "ContinueReprocess": "'"$ContinueReprocess"'",
        "ProcessTimestamp": "'"$processing_started_date_time_denver"'",
        "ProcessUser": "'"$processing_started_by_user"'"
      }'


if [ $? -eq 0 ]; then
  echo "
  ### SUCCESS - Job successfully finished
  "
  if [ $IsReprocess -eq 1 ] && [ $ContinueReprocess -eq 1 ]; then
    hive -e "INSERT OVERWRITE TABLE $tmp_rd VALUES('$extract_before_date');" ;
    echo -e "\n\n\tNow populating run-date table: $tmp_rd\n\twith $extract_before_date\n\tfor the next reprocess execution.\n\n"
  fi ;
else
  echo "
  ### ERROR: Job ended unsuccessfully -- Please re-run
  " && exit 1
fi ;
