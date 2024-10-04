#!/bin/bash
# Also, note that cadence is set in the evironment that process_dates.sh is called.
# CADENCE = [daily, fiscal_monthly, monthly, weekly]
export pwd=`pwd`
echo $pwd
echo -e "\n\n### Process Dates is starting, in the following directory:  $pwd\n\n"

if [ -z "$LAG_DAYS" ]
  then export LAGDAYS=1
elif [ "$LAG_DAYS" -ge 0 ]
  then export LAGDAYS="$LAG_DAYS"
else echo -e "\n\n### ERROR: Job ended unsuccessfully --\n\$LAG_DAYS: is not a valid number: \nPlease revise $LAG_DAYS to a valid number in the execute_hql.sh and try again.\n\n"
  exit 1
fi

if [ -z "$LEAD_DAYS" ]
  then export LEADDAYS=1
elif [ "$LEAD_DAYS" -ge 0 ]
  then export LEADDAYS="$LEAD_DAYS"
else echo -e "\n\n### ERROR: Job ended unsuccessfully --\n\$LEAD_DAYS: is not a valid number: \nPlease revise $LEAD_DAYS to a valid number in the execute_hql.sh and try again.\n\n"
  exit 1
fi

if [[ -f unix_timezones.tsv ]]; then
  #prepare time zones variable and trim last extraneous pipe
  cat unix_timezones.tsv | tr '\n' '|' > tzp
  export utz_terminalpipe=$(<tzp)
  export unixtimezones=${utz_terminalpipe%?}
else echo -e "\n\n### ERROR: Job ended unsuccessfully --\n\ Please make sure the time zone file, unix_timezones.tsv, is available and try again.\n\n"
  exit 1
fi

#echo $unixtimezones

if [ -z "$TIME_ZONE" ]
  then export TZ=America/Denver
elif [[ "$TIME_ZONE" =~ ${unixtimezones} ]]
  then export TZ="$TIME_ZONE"
else echo -e "\n\n### ERROR: Job ended unsuccessfully --\n\$TIME_ZONE: is not a valid value: \nPlease revise $TIME_ZONE to a valid value in the execute_hql.sh, make sure unix_timezones.tsv is available and try again.\n\n"
  exit 1
fi

#checks that cadence is a valid value
regexp='daily|weekly|monthly|fiscal_monthly';
if [ -z "${CADENCE}" ] || [[ ! $CADENCE =~ $regexp ]]; then
  echo -e "\n\n### ERROR: Job ended unsuccessfully --\n\ $CADENCE: is not a valid value: \nPlease revise $CADENCE to a valid value in the execute_hql.sh and try again.\n CADENCE = [daily, fiscal_monthly, monthly, weekly] \n\n"
    exit 1
fi

export GRAIN=$CADENCE

export extract_run_date=`date --date="$RUN_DATE - $LAGDAYS day" +%Y-%m-%d`

# Check that date is valid, and keeps quiet unless there is an issue.
date -d "$extract_run_date" >/dev/null
if [ "$?" -ne 0 ] || [ ${#extract_run_date} -ne 10 ] ; then
  echo -e "\n\n### ERROR: Job ended unsuccessfully --\n\$extract_run_date: is not a valid date: $extract_run_date\nPlease revise the computations that prepare the date and try again.\n\n"
  exit 1
fi

# starting materials echo
echo -n "
  TIME_ZONE        = $TZ
  CADENCE          = $CADENCE
  LEADDAYS         = $LEADDAYS
  LAGDAYS          = $LAGDAYS
  RUN_DATE         = $RUN_DATE
  extract_run_date = $extract_run_date

"

#Calendar Monthly END DATES include the first day of the next month for TZ conversion from UTC to local
export LAST_MONTH_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 month + 1 day" +%F)
export LAST_MONTH_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 1 day" +%F)
export LAST_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$LAST_MONTH_START_DATE `TZ="$TZ" date -d "$LAST_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$LAST_MONTH_END_DATE `TZ="$TZ" date -d "$LAST_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_MONTH_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days " +%F)

#(prev 8 days) Monthly END DATES include appropriate hours for TZ conversion from UTC to local
export PRIOR_8DAYS_LAST_MONTH_START_DATE=$(date -d "$LAST_MONTH_START_DATE - 8 day" +%F)
export PRIOR_8DAYS_LAST_MONTH_END_DATE=$(date -d "$LAST_MONTH_END_DATE - 8 day" +%F)
export PRIOR_8DAYS_LAST_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$PRIOR_8DAYS_LAST_MONTH_START_DATE `TZ="$TZ" date -d "$PRIOR_8DAYS_LAST_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export PRIOR_8DAYS_LAST_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$PRIOR_8DAYS_LAST_MONTH_END_DATE `TZ="$TZ" date -d "$PRIOR_8DAYS_LAST_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_MONTH_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 1 day " +%F)
export CURRENT_MONTH_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 1 month + 1 day" +%F)
export CURRENT_MONTH_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 1 month" +%F)
export CURRENT_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_MONTH_START_DATE `TZ="$TZ" date -d "$CURRENT_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_MONTH_END_DATE `TZ="$TZ" date -d "$CURRENT_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Fiscal Monthly 21st (FM21) END DATES include the first day of the next fiscal month for TZ conversion from UTC to local (21st is last day of fiscal month)
export LAST_FM21_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 2 MONTH + 22 day" +%F)
export LAST_FM21_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 22 day" +%F)
export LAST_FM21_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 21 day" +%F)
export LAST_FM21_START_DATE_TZ=$(TZ=UTC date -d "$LAST_FM21_START_DATE `TZ="$TZ" date -d "$LAST_FM21_START_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_FM21_END_DATE_TZ=$(TZ=UTC date -d "$LAST_FM21_END_DATE `TZ="$TZ" date -d "$LAST_FM21_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_FM21_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 22 day " +%F)
export CURRENT_FM21_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 22 day" +%F)
export CURRENT_FM21_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 21 day" +%F)
export CURRENT_FM21_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FM21_START_DATE `TZ="$TZ" date -d "$CURRENT_FM21_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_FM21_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FM21_END_DATE `TZ="$TZ" date -d "$CURRENT_FM21_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Fiscal Monthly 28th (FM28) END DATES include the first day of the next fiscal month for TZ conversion from UTC to local (28st is last day of fiscal month)
export LAST_FM28_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 2 MONTH + 29 day" +%F)
export LAST_FM28_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 29 day" +%F)
export LAST_FM28_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 28 day" +%F)
export LAST_FM28_START_DATE_TZ=$(TZ=UTC date -d "$LAST_FM28_START_DATE `TZ="$TZ" date -d "$LAST_FM28_START_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_FM28_END_DATE_TZ=$(TZ=UTC date -d "$LAST_FM28_END_DATE `TZ="$TZ" date -d "$LAST_FM28_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_FM28_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 29 day " +%F)
export CURRENT_FM28_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 29 day" +%F)
export CURRENT_FM28_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 28 day" +%F)
export CURRENT_FM28_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FM28_START_DATE `TZ="$TZ" date -d "$CURRENT_FM28_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_FM28_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FM28_END_DATE `TZ="$TZ" date -d "$CURRENT_FM28_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Fiscal Monthly 2019-08 Extended (FM201908X) END DATES include the first day of the next fiscal month for TZ conversion from UTC to local
#This accounts for business-imposed irregular fiscal month of 8/2019:
#"August fiscal month will be extended about a week longer going from 7/22-8/28"
export FM201908X_START_DATE="2019-07-22";
export FM201908X_END_DATE="2019-08-29"
export FM201908X_LABEL_DATE="2019-08-28"
export FM201908X_START_DATE_TZ="2019-07-22_06";
export FM201908X_END_DATE_TZ="2019-08-29_06"

#Fiscal Monthly END DATES include the first day of the next fiscal month for TZ conversion from UTC to local (21st is last day of fiscal month)
export LAST_FISCAL_MONTH_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 2 MONTH + 22 day" +%F)
export LAST_FISCAL_MONTH_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 22 day" +%F)
export LAST_FISCAL_MONTH_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 21 day" +%F)
export LAST_FISCAL_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$LAST_FISCAL_MONTH_START_DATE `TZ="$TZ" date -d "$LAST_FISCAL_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export LAST_FISCAL_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$LAST_FISCAL_MONTH_END_DATE `TZ="$TZ" date -d "$LAST_FISCAL_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_FISCAL_MONTH_START_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days - 1 MONTH + 22 day " +%F)
export CURRENT_FISCAL_MONTH_END_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 22 day" +%F)
export CURRENT_FISCAL_MONTH_LABEL_DATE=$(date -d "$extract_run_date -$(date -d $extract_run_date +%d) days + 21 day" +%F)
export CURRENT_FISCAL_MONTH_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FISCAL_MONTH_START_DATE `TZ="$TZ" date -d "$CURRENT_FISCAL_MONTH_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_FISCAL_MONTH_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_FISCAL_MONTH_END_DATE `TZ="$TZ" date -d "$CURRENT_FISCAL_MONTH_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Daily END DATES include appropriate hours for TZ conversion from UTC to local
export PRIOR_DAY_START_DATE=$(date -d "$extract_run_date - 1 day" +%F)
export PRIOR_DAY_END_DATE="$extract_run_date"
export PRIOR_DAY_START_DATE_TZ=$(TZ=UTC date -d "$PRIOR_DAY_START_DATE `TZ="$TZ" date -d "$PRIOR_DAY_START_DATE" +%Z`" +%Y-%m-%d_%H)
export PRIOR_DAY_END_DATE_TZ=$(TZ=UTC date -d "$PRIOR_DAY_END_DATE `TZ="$TZ" date -d "$PRIOR_DAY_END_DATE" +%Z`" +%Y-%m-%d_%H)

export CURRENT_DAY_START_DATE="$extract_run_date"
export CURRENT_DAY_END_DATE=$(date -d "$extract_run_date + $LEADDAYS day" +%F)
export CURRENT_DAY_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_DAY_START_DATE `TZ="$TZ" date -d "$CURRENT_DAY_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_DAY_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_DAY_END_DATE `TZ="$TZ" date -d "$CURRENT_DAY_END_DATE" +%Z`" +%Y-%m-%d_%H)

#Weekly END Dates are a running-7-day lag, including appropriate hours for TZ conversion from UTC to local
export CURRENT_WEEK_START_DATE=`date --date="$extract_run_date - 6 day" +%Y-%m-%d`
export CURRENT_WEEK_END_DATE=`date --date="$extract_run_date + 1 day" +%Y-%m-%d`
export LABEL_DATE_DENVER="$extract_run_date"
export CURRENT_WEEK_START_DATE_TZ=$(TZ=UTC date -d "$CURRENT_WEEK_START_DATE `TZ="$TZ" date -d "$CURRENT_WEEK_START_DATE" +%Z`" +%Y-%m-%d_%H)
export CURRENT_WEEK_END_DATE_TZ=$(TZ=UTC date -d "$CURRENT_WEEK_END_DATE `TZ="$TZ" date -d "$CURRENT_WEEK_END_DATE" +%Z`" +%Y-%m-%d_%H)

export processing_started_date_time_denver=$(TZ=America/Denver date -d @$(date -u +%s) "+%Y-%m-%d %H:%M:%S")
export processing_started_by_user=$(id -un)

#Fiscal Monthly considerations below
export Aug2019_epoch_seconds=$(date --date="2019-08-01" +%s) #1564617600
export CMSD_epoch_seconds=$(date --date="$CURRENT_MONTH_START_DATE" +%s)
export Sep2019_epoch_seconds=$(date --date="2019-09-01" +%s) #1567296000

if [ $CMSD_epoch_seconds -eq $Aug2019_epoch_seconds ]; then
  export CURRENT_FISCAL_MONTH_START_DATE=$FM201908X_START_DATE;
  export CURRENT_FISCAL_MONTH_END_DATE=$FM201908X_END_DATE;
  export CURRENT_FISCAL_MONTH_LABEL_DATE=$FM201908X_LABEL_DATE;
  export CURRENT_FISCAL_MONTH_START_DATE_TZ=$FM201908X_START_DATE_TZ;
  export CURRENT_FISCAL_MONTH_END_DATE_TZ=$FM201908X_END_DATE_TZ;

  export LAST_FISCAL_MONTH_START_DATE=$LAST_FM21_START_DATE;
  export LAST_FISCAL_MONTH_END_DATE=$LAST_FM21_END_DATE;
  export LAST_FISCAL_MONTH_LABEL_DATE=$LAST_FM21_LABEL_DATE;
  export LAST_FISCAL_MONTH_START_DATE_TZ=$LAST_FM21_START_DATE_TZ;
  export LAST_FISCAL_MONTH_END_DATE_TZ=$LAST_FM21_END_DATE_TZ;

elif [ $CMSD_epoch_seconds -eq $Sep2019_epoch_seconds ]; then
  export CURRENT_FISCAL_MONTH_START_DATE=$CURRENT_FM28_START_DATE;
  export CURRENT_FISCAL_MONTH_END_DATE=$CURRENT_FM28_END_DATE;
  export CURRENT_FISCAL_MONTH_LABEL_DATE=$CURRENT_FM28_LABEL_DATE;
  export CURRENT_FISCAL_MONTH_START_DATE_TZ=$CURRENT_FM28_START_DATE_TZ;
  export CURRENT_FISCAL_MONTH_END_DATE_TZ=$CURRENT_FM28_END_DATE_TZ;

  export LAST_FISCAL_MONTH_START_DATE=$FM201908X_START_DATE;
  export LAST_FISCAL_MONTH_END_DATE=$FM201908X_END_DATE;
  export LAST_FISCAL_MONTH_LABEL_DATE=$FM201908X_LABEL_DATE;
  export LAST_FISCAL_MONTH_START_DATE_TZ=$FM201908X_START_DATE_TZ;
  export LAST_FISCAL_MONTH_END_DATE_TZ=$FM201908X_END_DATE_TZ;

elif [ $CMSD_epoch_seconds -gt $Sep2019_epoch_seconds ]; then
  export CURRENT_FISCAL_MONTH_START_DATE=$CURRENT_FM28_START_DATE;
  export CURRENT_FISCAL_MONTH_END_DATE=$CURRENT_FM28_END_DATE;
  export CURRENT_FISCAL_MONTH_LABEL_DATE=$CURRENT_FM28_LABEL_DATE;
  export CURRENT_FISCAL_MONTH_START_DATE_TZ=$CURRENT_FM28_START_DATE_TZ;
  export CURRENT_FISCAL_MONTH_END_DATE_TZ=$CURRENT_FM28_END_DATE_TZ;

  export LAST_FISCAL_MONTH_START_DATE=$LAST_FM28_START_DATE;
  export LAST_FISCAL_MONTH_END_DATE=$LAST_FM28_END_DATE;
  export LAST_FISCAL_MONTH_LABEL_DATE=$LAST_FM28_LABEL_DATE;
  export LAST_FISCAL_MONTH_START_DATE_TZ=$LAST_FM28_START_DATE_TZ;
  export LAST_FISCAL_MONTH_END_DATE_TZ=$LAST_FM28_END_DATE_TZ;

elif [ $CMSD_epoch_seconds -lt $Aug2019_epoch_seconds ]; then
  export CURRENT_FISCAL_MONTH_START_DATE=$CURRENT_FM21_START_DATE;
  export CURRENT_FISCAL_MONTH_END_DATE=$CURRENT_FM21_END_DATE;
  export CURRENT_FISCAL_MONTH_LABEL_DATE=$CURRENT_FM21_LABEL_DATE;
  export CURRENT_FISCAL_MONTH_START_DATE_TZ=$CURRENT_FM21_START_DATE_TZ;
  export CURRENT_FISCAL_MONTH_END_DATE_TZ=$CURRENT_FM21_END_DATE_TZ;

  export LAST_FISCAL_MONTH_START_DATE=$LAST_FM21_START_DATE;
  export LAST_FISCAL_MONTH_END_DATE=$LAST_FM21_END_DATE;
  export LAST_FISCAL_MONTH_LABEL_DATE=$LAST_FM21_LABEL_DATE;
  export LAST_FISCAL_MONTH_START_DATE_TZ=$LAST_FM21_START_DATE_TZ;
  export LAST_FISCAL_MONTH_END_DATE_TZ=$LAST_FM21_END_DATE_TZ;
else
  echo "
  ### ERROR: Job ended unsuccessfully --
  Something unexpected occurred when figuring out lumpy fiscal months...

  It turned out that comparing epoch seconds for Aug and Sep 2019 is more
  challenging then it appears.

  "
  exit 1
fi


#Cadence below, from externally exported CADENCE environment variable
if   [ $CADENCE == "monthly" ]; then
  export START_DATE_TZ="$CURRENT_MONTH_START_DATE_TZ" ;
  export END_DATE_TZ="$CURRENT_MONTH_END_DATE_TZ" ;
  export START_DATE="$CURRENT_MONTH_START_DATE" ;
  export END_DATE="$CURRENT_MONTH_END_DATE" ;
  export PRIOR_8DAYS_START_DATE="$PRIOR_8DAYS_LAST_MONTH_START_DATE" ;
  export PRIOR_8DAYS_END_DATE="$PRIOR_8DAYS_LAST_MONTH_END_DATE" ;
  export LABEL_DATE_DENVER="$CURRENT_MONTH_LABEL_DATE";
  echo -e "\n\nNow processing the CURRENT CALENDAR month ending → $CURRENT_MONTH_LABEL_DATE\n\n";

elif [ $CADENCE == "fiscal_monthly" ]; then
  export START_DATE_TZ="$CURRENT_FISCAL_MONTH_START_DATE_TZ" ;
  export END_DATE_TZ="$CURRENT_FISCAL_MONTH_END_DATE_TZ" ;
  export START_DATE="$CURRENT_FISCAL_MONTH_START_DATE" ;
  export END_DATE="$CURRENT_FISCAL_MONTH_END_DATE" ;
  export LABEL_DATE_DENVER="$CURRENT_FISCAL_MONTH_LABEL_DATE"
  echo -e "\n\nNow processing the FISCAL_MONTH ending → $CURRENT_FISCAL_MONTH_LABEL_DATE\n\n";

elif [ $CADENCE == "daily" ]; then
  export START_DATE_TZ="$CURRENT_DAY_START_DATE_TZ" ;
  export END_DATE_TZ="$CURRENT_DAY_END_DATE_TZ" ;
  export START_DATE="$CURRENT_DAY_START_DATE" ;
  export END_DATE="$CURRENT_DAY_END_DATE" ;
  export PRIOR_8DAYS_START_DATE="$PRIOR_8DAYS_START_DATE" ;
  export PRIOR_8DAYS_END_DATE="$PRIOR_8DAYS_END_DATE" ;
  export LABEL_DATE_DENVER="$START_DATE"
  echo -e "\n\nNow processing the PRIOR day → $extract_run_date\n\n";

# just to be clear, weekly is actually rolling seven days
elif [ $CADENCE == "weekly" ]; then
  export START_DATE_TZ="$CURRENT_WEEK_START_DATE_TZ" ;
  export END_DATE_TZ="$CURRENT_WEEK_END_DATE_TZ" ;
  export START_DATE="$CURRENT_WEEK_START_DATE" ;
  export END_DATE="$CURRENT_WEEK_END_DATE" ;
  echo -e "\n\nNow processing the WEEK ending on the PRIOR day → $extract_run_date\n\n";

else
  echo "
  ### ERROR: UNDEFINED CADENCE -- Please re-run
  " && exit 1
fi ;

echo '      {
        "START_DATE":               "'"$START_DATE"'",
        "END_DATE":                 "'"$END_DATE"'",
        "START_DATE_TZ":            "'"$START_DATE_TZ"'",
        "END_DATE_TZ":              "'"$END_DATE_TZ"'",
        "PRIOR_8DAYS_START_DATE":   "'"$PRIOR_8DAYS_START_DATE"'",
        "PRIOR_8DAYS_END_DATE":     "'"$PRIOR_8DAYS_END_DATE"'",
        "CADENCE":                  "'"$CADENCE"'",
        "LABEL_DATE_DENVER":        "'"$LABEL_DATE_DENVER"'",
        "ProcessTimestamp":         "'"$processing_started_date_time_denver"'",
        "ProcessUser":              "'"$processing_started_by_user"'"
      }'

if [ $? -eq 0 ]; then
  echo "
  ### SUCCESS - The Process Dates script successfully finished
  "
else
  echo "
  ### ERROR: The Process Dates script ended unsuccessfully -- Please re-run after sorting through what happened.
  " && exit 1
fi ;
