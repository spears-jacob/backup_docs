#the following in its own shell to export to filename/test number

today=$(date +"%Y_%m_%d")

#RUN_DATE
#TIME_ZONE
#START_DATE
#END_DATE
#START_DATE_TZ
#END_DATE_TZ
#label_date_denver
#LAG_DAYS
#LEAD_DAYS

## Check expected vs actual outcome for daily
#group_array=( $a01 $a02 $a03 $a04 $a05 $a06 $a07 $a08 $a09 $a10)
#for g in "${group_array[@]}"
#do
#if [ $\DAILY_EXPECTED_$g=$\DAILY_$g ]
#  then echo "### PASS: DAILY_$g ###"
#else echo -e "### ERROR: DAILY_'$g' does not match expected outcome. Please troubleshoot DAILY_'$g' variable and try again. ###"
#fi
#done
echo -e "### Running test outputs for DAILY cadence ###\n\n"

if [ $DAILY_EXPECTED_RUN_DATE == $DAILY_RUN_DATE ]
  then echo -e "### Expected: $DAILY_EXPECTED_RUN_DATE ::: Actual: $DAILY_RUN_DATE ### \n### PASS: DAILY_RUN_DATE ###\n\n"
  else echo -e "### Expected: $DAILY_EXPECTED_RUN_DATE ::: Actual: $DAILY_RUN_DATE ### \n### ERROR: DAILY_RUN_DATE does not match expected outcome. Please troubleshoot DAILY_RUN_DATE variable and try again. ###\n\n"
fi

if [ $DAILY_EXPECTED_TIME_ZONE == $DAILY_TIME_ZONE ]
  then echo -e "### Expected: $DAILY_EXPECTED_TIME_ZONE ::: Actual: $DAILY_TIME_ZONE ### \n### PASS: DAILY_TIME_ZONE ###\n\n"
  else echo -e "### Expected: $DAILY_EXPECTED_TIME_ZONE ::: Actual: $DAILY_TIME_ZONE ### \n### ERROR: DAILY_TIME_ZONE does not match expected outcome. Please troubleshoot DAILY_TIME_ZONE variable and try again. ###\n\n"
fi

if [ $DAILY_EXPECTED_START_DATE == $DAILY_START_DATE ]
  then echo -e "### Expected: $DAILY_EXPECTED_START_DATE ::: Actual: $DAILY_START_DATE ### \n### PASS: DAILY_START_DATE ###\n\n"
  else echo -e "### Expected: $DAILY_EXPECTED_START_DATE ::: Actual: $DAILY_START_DATE ### \n### ERROR: DAILY_START_DATE does not match expected outcome. Please troubleshoot DAILY_START_DATE variable and try again. ###\n\n"
fi

if [ $DAILY_EXPECTED_END_DATE == $DAILY_END_DATE ]
  then echo -e "### Expected: $DAILY_EXPECTED_END_DATE ::: Actual: $DAILY_END_DATE ### \n### PASS: DAILY_END_DATE ###\n\n"
  else echo -e "### Expected: $DAILY_EXPECTED_END_DATE ::: Actual: $DAILY_END_DATE ### \n### ERROR: DAILY_END_DATE does not match expected outcome. Please troubleshoot DAILY_END_DATE variable and try again. ###\n\n"
fi

if [ $DAILY_EXPECTED_START_DATE_TZ == $DAILY_START_DATE_TZ ]
  then echo -e "### Expected: $DAILY_EXPECTED_START_DATE_TZ ::: Actual: $DAILY_START_DATE_TZ ### \n### PASS: DAILY_START_DATE_TZ ###\n\n"
  else echo -e "### Expected: $DAILY_EXPECTED_START_DATE_TZ ::: Actual: $DAILY_START_DATE_TZ ### \n### ERROR: DAILY_START_DATE_TZ does not match expected outcome. Please troubleshoot DAILY_START_DATE_TZ variable and try again. ###\n\n"
fi

if [ $DAILY_EXPECTED_END_DATE_TZ == $DAILY_END_DATE_TZ ]
  then echo -e "### Expected: $DAILY_EXPECTED_END_DATE_TZ ::: Actual: $DAILY_END_DATE_TZ ### \n### PASS: DAILY_END_DATE_TZ ###\n\n"
  else echo -e "### Expected: $DAILY_EXPECTED_END_DATE_TZ ::: Actual: $DAILY_END_DATE_TZ ### \n### ERROR: DAILY_END_DATE_TZ does not match expected outcome. Please troubleshoot DAILY_END_DATE_TZ variable and try again. ###\n\n"
fi

if [ $DAILY_EXPECTED_label_date_denver == $DAILY_label_date_denver ]
  then echo -e "### Expected: $DAILY_EXPECTED_label_date_denver ::: Actual: $DAILY_label_date_denver ### \n### PASS: DAILY_label_date_denver ###\n\n"
  else echo -e "### Expected: $DAILY_EXPECTED_label_date_denver ::: Actual: $DAILY_label_date_denver ### \n### ERROR: DAILY_label_date_denver does not match expected outcome. Please troubleshoot DAILY_label_date_denver variable and try again. ###\n\n"
fi

if [ $DAILY_EXPECTED_LAG_DAYS -eq $DAILY_LAG_DAYS ]
  then echo -e "### Expected: $DAILY_EXPECTED_LAG_DAYS ::: Actual: $DAILY_LAG_DAYS ### \n### PASS: DAILY_LAG_DAYS ###\n\n"
  else echo -e "### Expected: $DAILY_EXPECTED_LAG_DAYS ::: Actual: $DAILY_LAG_DAYS ### \n### ERROR: DAILY_LAG_DAYS does not match expected outcome. Please troubleshoot DAILY_LAG_DAYS variable and try again. ###\n\n"
fi

if [ $DAILY_EXPECTED_LEAD_DAYS -eq $DAILY_LEAD_DAYS ]
  then echo -e "### Expected: $DAILY_EXPECTED_LEAD_DAYS ::: Actual: $DAILY_LEAD_DAYS ### \n### PASS: DAILY_LEAD_DAYS ###\n\n"
  else echo -e "### Expected: $DAILY_EXPECTED_LEAD_DAYS ::: Actual: $DAILY_LEAD_DAYS ### \n### ERROR: DAILY_LEAD_DAYS does not match expected outcome. Please troubleshoot DAILY_LEAD_DAYS variable and try again. ###\n\n"
fi

###############
echo -e "### Running test outputs for DAILY_LAGLEAD cadence ###\n\n"

if [ $DAILY_LAGLEAD_EXPECTED_RUN_DATE == $DAILY_LAGLEAD_RUN_DATE ]
  then echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_RUN_DATE ::: Actual: $DAILY_LAGLEAD_RUN_DATE ### \n### PASS: DAILY_LAGLEAD_RUN_DATE ###\n\n"
  else echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_RUN_DATE ::: Actual: $DAILY_LAGLEAD_RUN_DATE ### \n### ERROR: DAILY_LAGLEAD_RUN_DATE does not match expected outcome. Please troubleshoot DAILY_LAGLEAD_RUN_DATE variable and try again. ###\n\n"
fi

if [ $DAILY_LAGLEAD_EXPECTED_TIME_ZONE == $DAILY_LAGLEAD_TIME_ZONE ]
  then echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_TIME_ZONE ::: Actual: $DAILY_LAGLEAD_TIME_ZONE ### \n### PASS: DAILY_LAGLEAD_TIME_ZONE ###\n\n"
  else echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_TIME_ZONE ::: Actual: $DAILY_LAGLEAD_TIME_ZONE ### \n### ERROR: DAILY_LAGLEAD_TIME_ZONE does not match expected outcome. Please troubleshoot DAILY_LAGLEAD_TIME_ZONE variable and try again. ###\n\n"
fi

if [ $DAILY_LAGLEAD_EXPECTED_START_DATE == $DAILY_LAGLEAD_START_DATE ]
  then echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_START_DATE ::: Actual: $DAILY_LAGLEAD_START_DATE ### \n### PASS: DAILY_LAGLEAD_START_DATE ###\n\n"
  else echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_START_DATE ::: Actual: $DAILY_LAGLEAD_START_DATE ### \n### ERROR: DAILY_LAGLEAD_START_DATE does not match expected outcome. Please troubleshoot DAILY_LAGLEAD_START_DATE variable and try again. ###\n\n"
fi

if [ $DAILY_LAGLEAD_EXPECTED_END_DATE == $DAILY_LAGLEAD_END_DATE ]
  then echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_END_DATE ::: Actual: $DAILY_LAGLEAD_END_DATE ### \n### PASS: DAILY_LAGLEAD_END_DATE ###\n\n"
  else echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_END_DATE ::: Actual: $DAILY_LAGLEAD_END_DATE ### \n### ERROR: DAILY_LAGLEAD_END_DATE does not match expected outcome. Please troubleshoot DAILY_LAGLEAD_END_DATE variable and try again. ###\n\n"
fi

if [ $DAILY_LAGLEAD_EXPECTED_START_DATE_TZ == $DAILY_LAGLEAD_START_DATE_TZ ]
  then echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_START_DATE_TZ ::: Actual: $DAILY_LAGLEAD_START_DATE_TZ ### \n### PASS: DAILY_LAGLEAD_START_DATE_TZ ###\n\n"
  else echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_START_DATE_TZ ::: Actual: $DAILY_LAGLEAD_START_DATE_TZ ### \n### ERROR: DAILY_LAGLEAD_START_DATE_TZ does not match expected outcome. Please troubleshoot DAILY_LAGLEAD_START_DATE_TZ variable and try again. ###\n\n"
fi

if [ $DAILY_LAGLEAD_EXPECTED_END_DATE_TZ == $DAILY_LAGLEAD_END_DATE_TZ ]
  then echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_END_DATE_TZ ::: Actual: $DAILY_LAGLEAD_END_DATE_TZ ### \n### PASS: DAILY_LAGLEAD_END_DATE_TZ ###\n\n"
  else echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_END_DATE_TZ ::: Actual: $DAILY_LAGLEAD_END_DATE_TZ ### \n### ERROR: DAILY_LAGLEAD_END_DATE_TZ does not match expected outcome. Please troubleshoot DAILY_LAGLEAD_END_DATE_TZ variable and try again. ###\n\n"
fi

if [ $DAILY_LAGLEAD_EXPECTED_label_date_denver == $DAILY_LAGLEAD_label_date_denver ]
  then echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_label_date_denver ::: Actual: $DAILY_LAGLEAD_label_date_denver ### \n### PASS: DAILY_LAGLEAD_label_date_denver ###\n\n"
  else echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_label_date_denver ::: Actual: $DAILY_LAGLEAD_label_date_denver ### \n### ERROR: DAILY_LAGLEAD_label_date_denver does not match expected outcome. Please troubleshoot DAILY_LAGLEAD_label_date_denver variable and try again. ###\n\n"
fi

if [ $DAILY_LAGLEAD_EXPECTED_LAG_DAYS -eq $DAILY_LAGLEAD_LAG_DAYS ]
  then echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_LAG_DAYS ::: Actual: $DAILY_LAGLEAD_LAG_DAYS ### \n### PASS: DAILY_LAGLEAD_LAG_DAYS ###\n\n"
  else echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_LAG_DAYS ::: Actual: $DAILY_LAGLEAD_LAG_DAYS ### \n### ERROR: DAILY_LAGLEAD_LAG_DAYS does not match expected outcome. Please troubleshoot DAILY_LAGLEAD_LAG_DAYS variable and try again. ###\n\n"
fi

if [ $DAILY_LAGLEAD_EXPECTED_LEAD_DAYS -eq $DAILY_LAGLEAD_LEAD_DAYS ]
  then echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_LEAD_DAYS ::: Actual: $DAILY_LAGLEAD_LEAD_DAYS ### \n### PASS: DAILY_LAGLEAD_LEAD_DAYS ###\n\n"
  else echo -e "### Expected: $DAILY_LAGLEAD_EXPECTED_LEAD_DAYS ::: Actual: $DAILY_LAGLEAD_LEAD_DAYS ### \n### ERROR: DAILY_LAGLEAD_LEAD_DAYS does not match expected outcome. Please troubleshoot DAILY_LAGLEAD_LEAD_DAYS variable and try again. ###\n\n"
fi

###############
echo -e "### Running test outputs for WEEKLY cadence ###\n\n"

if [ $WEEKLY_EXPECTED_RUN_DATE == $WEEKLY_RUN_DATE ]
  then echo -e "### Expected: $WEEKLY_EXPECTED_RUN_DATE ::: Actual: $WEEKLY_RUN_DATE ### \n### PASS: WEEKLY_RUN_DATE ###\n\n"
  else echo -e "### Expected: $WEEKLY_EXPECTED_RUN_DATE ::: Actual: $WEEKLY_RUN_DATE ### \n### ERROR: WEEKLY_RUN_DATE does not match expected outcome. Please troubleshoot WEEKLY_RUN_DATE variable and try again. ###\n\n"
fi

if [ $WEEKLY_EXPECTED_TIME_ZONE == $WEEKLY_TIME_ZONE ]
  then echo -e "### Expected: $WEEKLY_EXPECTED_TIME_ZONE ::: Actual: $WEEKLY_TIME_ZONE ### \n### PASS: WEEKLY_TIME_ZONE ###\n\n"
  else echo -e "### Expected: $WEEKLY_EXPECTED_TIME_ZONE ::: Actual: $WEEKLY_TIME_ZONE ### \n### ERROR: WEEKLY_TIME_ZONE does not match expected outcome. Please troubleshoot WEEKLY_TIME_ZONE variable and try again. ###\n\n"
fi

if [ $WEEKLY_EXPECTED_START_DATE == $WEEKLY_START_DATE ]
  then echo -e "### Expected: $WEEKLY_EXPECTED_START_DATE ::: Actual: $WEEKLY_START_DATE ### \n### PASS: WEEKLY_START_DATE ###\n\n"
  else echo -e "### Expected: $WEEKLY_EXPECTED_START_DATE ::: Actual: $WEEKLY_START_DATE ### \n### ERROR: WEEKLY_START_DATE does not match expected outcome. Please troubleshoot WEEKLY_START_DATE variable and try again. ###\n\n"
fi

if [ $WEEKLY_EXPECTED_END_DATE == $WEEKLY_END_DATE ]
  then echo -e "### Expected: $WEEKLY_EXPECTED_END_DATE ::: Actual: $WEEKLY_END_DATE ### \n### PASS: WEEKLY_END_DATE ###\n\n"
  else echo -e "### Expected: $WEEKLY_EXPECTED_END_DATE ::: Actual: $WEEKLY_END_DATE ### \n### ERROR: WEEKLY_END_DATE does not match expected outcome. Please troubleshoot WEEKLY_END_DATE variable and try again. ###\n\n"
fi

if [ $WEEKLY_EXPECTED_START_DATE_TZ == $WEEKLY_START_DATE_TZ ]
  then echo -e "### Expected: $WEEKLY_EXPECTED_START_DATE_TZ ::: Actual: $WEEKLY_START_DATE_TZ ### \n### PASS: WEEKLY_START_DATE_TZ ###\n\n"
  else echo -e "### Expected: $WEEKLY_EXPECTED_START_DATE_TZ ::: Actual: $WEEKLY_START_DATE_TZ ### \n### ERROR: WEEKLY_START_DATE_TZ does not match expected outcome. Please troubleshoot WEEKLY_START_DATE_TZ variable and try again. ###\n\n"
fi

if [ $WEEKLY_EXPECTED_END_DATE_TZ == $WEEKLY_END_DATE_TZ ]
  then echo -e "### Expected: $WEEKLY_EXPECTED_END_DATE_TZ ::: Actual: $WEEKLY_END_DATE_TZ ### \n### PASS: WEEKLY_END_DATE_TZ ###\n\n"
  else echo -e "### Expected: $WEEKLY_EXPECTED_END_DATE_TZ ::: Actual: $WEEKLY_END_DATE_TZ ### \n### ERROR: WEEKLY_END_DATE_TZ does not match expected outcome. Please troubleshoot WEEKLY_END_DATE_TZ variable and try again. ###\n\n"
fi

if [ $WEEKLY_EXPECTED_label_date_denver == $WEEKLY_label_date_denver ]
  then echo -e "### Expected: $WEEKLY_EXPECTED_label_date_denver ::: Actual: $WEEKLY_label_date_denver ### \n### PASS: WEEKLY_label_date_denver ###\n\n"
  else echo -e "### Expected: $WEEKLY_EXPECTED_label_date_denver ::: Actual: $WEEKLY_label_date_denver ### \n### ERROR: WEEKLY_label_date_denver does not match expected outcome. Please troubleshoot WEEKLY_label_date_denver variable and try again. ###\n\n"
fi

if [ $WEEKLY_EXPECTED_LAG_DAYS -eq $WEEKLY_LAG_DAYS ]
  then echo -e "### Expected: $WEEKLY_EXPECTED_LAG_DAYS ::: Actual: $WEEKLY_LAG_DAYS ### \n### PASS: WEEKLY_LAG_DAYS ###\n\n"
  else echo -e "### Expected: $WEEKLY_EXPECTED_LAG_DAYS ::: Actual: $WEEKLY_LAG_DAYS ### \n### ERROR: WEEKLY_LAG_DAYS does not match expected outcome. Please troubleshoot WEEKLY_LAG_DAYS variable and try again. ###\n\n"
fi

if [ $WEEKLY_EXPECTED_LEAD_DAYS -eq $WEEKLY_LEAD_DAYS ]
  then echo -e "### Expected: $WEEKLY_EXPECTED_LEAD_DAYS ::: Actual: $WEEKLY_LEAD_DAYS ### \n### PASS: WEEKLY_LEAD_DAYS ###\n\n"
  else echo -e "### Expected: $WEEKLY_EXPECTED_LEAD_DAYS ::: Actual: $WEEKLY_LEAD_DAYS ### \n### ERROR: WEEKLY_LEAD_DAYS does not match expected outcome. Please troubleshoot WEEKLY_LEAD_DAYS variable and try again. ###\n\n"
fi

###############
echo -e "### Running test outputs for MONTHLY cadence ###\n\n"

if [ $MONTHLY_EXPECTED_RUN_DATE == $MONTHLY_RUN_DATE ]
  then echo -e "### Expected: $MONTHLY_EXPECTED_RUN_DATE ::: Actual: $MONTHLY_RUN_DATE ### \n### PASS: MONTHLY_RUN_DATE ###\n\n"
  else echo -e "### Expected: $MONTHLY_EXPECTED_RUN_DATE ::: Actual: $MONTHLY_RUN_DATE ### \n### ERROR: MONTHLY_RUN_DATE does not match expected outcome. Please troubleshoot MONTHLY_RUN_DATE variable and try again. ###\n\n"
fi

if [ $MONTHLY_EXPECTED_TIME_ZONE == $MONTHLY_TIME_ZONE ]
  then echo -e "### Expected: $MONTHLY_EXPECTED_TIME_ZONE ::: Actual: $MONTHLY_TIME_ZONE ### \n### PASS: MONTHLY_TIME_ZONE ###\n\n"
  else echo -e "### Expected: $MONTHLY_EXPECTED_TIME_ZONE ::: Actual: $MONTHLY_TIME_ZONE ### \n### ERROR: MONTHLY_TIME_ZONE does not match expected outcome. Please troubleshoot MONTHLY_TIME_ZONE variable and try again. ###\n\n"
fi

if [ $MONTHLY_EXPECTED_START_DATE == $MONTHLY_START_DATE ]
  then echo -e "### Expected: $MONTHLY_EXPECTED_START_DATE ::: Actual: $MONTHLY_START_DATE ### \n### PASS: MONTHLY_START_DATE ###\n\n"
  else echo -e "### Expected: $MONTHLY_EXPECTED_START_DATE ::: Actual: $MONTHLY_START_DATE ### \n### ERROR: MONTHLY_START_DATE does not match expected outcome. Please troubleshoot MONTHLY_START_DATE variable and try again. ###\n\n"
fi

if [ $MONTHLY_EXPECTED_END_DATE == $MONTHLY_END_DATE ]
  then echo -e "### Expected: $MONTHLY_EXPECTED_END_DATE ::: Actual: $MONTHLY_END_DATE ### \n### PASS: MONTHLY_END_DATE ###\n\n"
  else echo -e "### Expected: $MONTHLY_EXPECTED_END_DATE ::: Actual: $MONTHLY_END_DATE ### \n### ERROR: MONTHLY_END_DATE does not match expected outcome. Please troubleshoot MONTHLY_END_DATE variable and try again. ###\n\n"
fi

if [ $MONTHLY_EXPECTED_START_DATE_TZ == $MONTHLY_START_DATE_TZ ]
  then echo -e "### Expected: $MONTHLY_EXPECTED_START_DATE_TZ ::: Actual: $MONTHLY_START_DATE_TZ ### \n### PASS: MONTHLY_START_DATE_TZ ###\n\n"
  else echo -e "### Expected: $MONTHLY_EXPECTED_START_DATE_TZ ::: Actual: $MONTHLY_START_DATE_TZ ### \n### ERROR: MONTHLY_START_DATE_TZ does not match expected outcome. Please troubleshoot MONTHLY_START_DATE_TZ variable and try again. ###\n\n"
fi

if [ $MONTHLY_EXPECTED_END_DATE_TZ == $MONTHLY_END_DATE_TZ ]
  then echo -e "### Expected: $MONTHLY_EXPECTED_END_DATE_TZ ::: Actual: $MONTHLY_END_DATE_TZ ### \n### PASS: MONTHLY_END_DATE_TZ ###\n\n"
  else echo -e "### Expected: $MONTHLY_EXPECTED_END_DATE_TZ ::: Actual: $MONTHLY_END_DATE_TZ ### \n### ERROR: MONTHLY_END_DATE_TZ does not match expected outcome. Please troubleshoot MONTHLY_END_DATE_TZ variable and try again. ###\n\n"
fi

if [ $MONTHLY_EXPECTED_label_date_denver == $MONTHLY_label_date_denver ]
  then echo -e "### Expected: $MONTHLY_EXPECTED_label_date_denver ::: Actual: $MONTHLY_label_date_denver ### \n### PASS: MONTHLY_label_date_denver ###\n\n"
  else echo -e "### Expected: $MONTHLY_EXPECTED_label_date_denver ::: Actual: $MONTHLY_label_date_denver ### \n### ERROR: MONTHLY_label_date_denver does not match expected outcome. Please troubleshoot MONTHLY_label_date_denver variable and try again. ###\n\n"
fi

if [ $MONTHLY_EXPECTED_LAG_DAYS -eq $MONTHLY_LAG_DAYS ]
  then echo -e "### Expected: $MONTHLY_EXPECTED_LAG_DAYS ::: Actual: $MONTHLY_LAG_DAYS ### \n### PASS: MONTHLY_LAG_DAYS ###\n\n"
  else echo -e "### Expected: $MONTHLY_EXPECTED_LAG_DAYS ::: Actual: $MONTHLY_LAG_DAYS ### \n### ERROR: MONTHLY_LAG_DAYS does not match expected outcome. Please troubleshoot MONTHLY_LAG_DAYS variable and try again. ###\n\n"
fi

if [ $MONTHLY_EXPECTED_LEAD_DAYS -eq $MONTHLY_LEAD_DAYS ]
  then echo -e "### Expected: $MONTHLY_EXPECTED_LEAD_DAYS ::: Actual: $MONTHLY_LEAD_DAYS ### \n### PASS: MONTHLY_LEAD_DAYS ###\n\n"
  else echo -e "### Expected: $MONTHLY_EXPECTED_LEAD_DAYS ::: Actual: $MONTHLY_LEAD_DAYS ### \n### ERROR: MONTHLY_LEAD_DAYS does not match expected outcome. Please troubleshoot MONTHLY_LEAD_DAYS variable and try again. ###\n\n"
fi

###############
echo -e "### Running test outputs for FISCAL_MONTHLY cadence ###\n\n"

if [ $FISCAL_MONTHLY_EXPECTED_RUN_DATE == $FISCAL_MONTHLY_RUN_DATE ]
  then echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_RUN_DATE ::: Actual: $FISCAL_MONTHLY_RUN_DATE ### \n### PASS: FISCAL_MONTHLY_RUN_DATE ###\n\n"
  else echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_RUN_DATE ::: Actual: $FISCAL_MONTHLY_RUN_DATE ### \n### ERROR: FISCAL_MONTHLY_RUN_DATE does not match expected outcome. Please troubleshoot FISCAL_MONTHLY_RUN_DATE variable and try again. ###\n\n"
fi

if [ $FISCAL_MONTHLY_EXPECTED_TIME_ZONE == $FISCAL_MONTHLY_TIME_ZONE ]
  then echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_TIME_ZONE ::: Actual: $FISCAL_MONTHLY_TIME_ZONE ### \n### PASS: FISCAL_MONTHLY_TIME_ZONE ###\n\n"
  else echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_TIME_ZONE ::: Actual: $FISCAL_MONTHLY_TIME_ZONE ### \n### ERROR: FISCAL_MONTHLY_TIME_ZONE does not match expected outcome. Please troubleshoot FISCAL_MONTHLY_TIME_ZONE variable and try again. ###\n\n"
fi

if [ $FISCAL_MONTHLY_EXPECTED_START_DATE == $FISCAL_MONTHLY_START_DATE ]
  then echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_START_DATE ::: Actual: $FISCAL_MONTHLY_START_DATE ### \n### PASS: FISCAL_MONTHLY_START_DATE ###\n\n"
  else echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_START_DATE ::: Actual: $FISCAL_MONTHLY_START_DATE ### \n### ERROR: FISCAL_MONTHLY_START_DATE does not match expected outcome. Please troubleshoot FISCAL_MONTHLY_START_DATE variable and try again. ###\n\n"
fi

if [ $FISCAL_MONTHLY_EXPECTED_END_DATE == $FISCAL_MONTHLY_END_DATE ]
  then echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_END_DATE ::: Actual: $FISCAL_MONTHLY_END_DATE ### \n### PASS: FISCAL_MONTHLY_END_DATE ###\n\n"
  else echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_END_DATE ::: Actual: $FISCAL_MONTHLY_END_DATE ### \n### ERROR: FISCAL_MONTHLY_END_DATE does not match expected outcome. Please troubleshoot FISCAL_MONTHLY_END_DATE variable and try again. ###\n\n"
fi

if [ $FISCAL_MONTHLY_EXPECTED_START_DATE_TZ == $FISCAL_MONTHLY_START_DATE_TZ ]
  then echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_START_DATE_TZ ::: Actual: $FISCAL_MONTHLY_START_DATE_TZ ### \n### PASS: FISCAL_MONTHLY_START_DATE_TZ ###\n\n"
  else echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_START_DATE_TZ ::: Actual: $FISCAL_MONTHLY_START_DATE_TZ ### \n### ERROR: FISCAL_MONTHLY_START_DATE_TZ does not match expected outcome. Please troubleshoot FISCAL_MONTHLY_START_DATE_TZ variable and try again. ###\n\n"
fi

if [ $FISCAL_MONTHLY_EXPECTED_END_DATE_TZ == $FISCAL_MONTHLY_END_DATE_TZ ]
  then echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_END_DATE_TZ ::: Actual: $FISCAL_MONTHLY_END_DATE_TZ ### \n### PASS: FISCAL_MONTHLY_END_DATE_TZ ###\n\n"
  else echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_END_DATE_TZ ::: Actual: $FISCAL_MONTHLY_END_DATE_TZ ### \n### ERROR: FISCAL_MONTHLY_END_DATE_TZ does not match expected outcome. Please troubleshoot FISCAL_MONTHLY_END_DATE_TZ variable and try again. ###\n\n"
fi

if [ $FISCAL_MONTHLY_EXPECTED_label_date_denver == $FISCAL_MONTHLY_label_date_denver ]
  then echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_label_date_denver ::: Actual: $FISCAL_MONTHLY_label_date_denver ### \n### PASS: FISCAL_MONTHLY_label_date_denver ###\n\n"
  else echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_label_date_denver ::: Actual: $FISCAL_MONTHLY_label_date_denver ### \n### ERROR: FISCAL_MONTHLY_label_date_denver does not match expected outcome. Please troubleshoot FISCAL_MONTHLY_label_date_denver variable and try again. ###\n\n"
fi

if [ $FISCAL_MONTHLY_EXPECTED_LAG_DAYS -eq $FISCAL_MONTHLY_LAG_DAYS ]
  then echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_LAG_DAYS ::: Actual: $FISCAL_MONTHLY_LAG_DAYS ### \n### PASS: FISCAL_MONTHLY_LAG_DAYS ###\n\n"
  else echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_LAG_DAYS ::: Actual: $FISCAL_MONTHLY_LAG_DAYS ### \n### ERROR: FISCAL_MONTHLY_LAG_DAYS does not match expected outcome. Please troubleshoot FISCAL_MONTHLY_LAG_DAYS variable and try again. ###\n\n"
fi

if [ $FISCAL_MONTHLY_EXPECTED_LEAD_DAYS -eq $FISCAL_MONTHLY_LEAD_DAYS ]
  then echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_LEAD_DAYS ::: Actual: $FISCAL_MONTHLY_LEAD_DAYS ### \n### PASS: FISCAL_MONTHLY_LEAD_DAYS ###\n\n"
  else echo -e "### Expected: $FISCAL_MONTHLY_EXPECTED_LEAD_DAYS ::: Actual: $FISCAL_MONTHLY_LEAD_DAYS ### \n### ERROR: FISCAL_MONTHLY_LEAD_DAYS does not match expected outcome. Please troubleshoot FISCAL_MONTHLY_LEAD_DAYS variable and try again. ###\n\n"
fi

#############
echo -e "### Exporting date values to text file. This will overwrite an existing file on this date ###\n\n"

echo "
CADENCE|VALUE|EXPECTED|ACTUAL
DAILY|RUN_DATE|$DAILY_EXPECTED_RUN_DATE|$DAILY_RUN_DATE
DAILY|TIME_ZONE|$DAILY_EXPECTED_TIME_ZONE|$DAILY_TIME_ZONE
DAILY|START_DATE|$DAILY_EXPECTED_START_DATE|$DAILY_START_DATE
DAILY|END_DATE|$DAILY_EXPECTED_END_DATE|$DAILY_END_DATE
DAILY|START_DATE_TZ|$DAILY_EXPECTED_START_DATE_TZ|$DAILY_START_DATE_TZ
DAILY|END_DATE_TZ|$DAILY_EXPECTED_END_DATE_TZ|$DAILY_END_DATE_TZ
DAILY|CADENCE|$DAILY_EXPECTED_CADENCE|$DAILY_CADENCE
DAILY|label_date_denver|$DAILY_EXPECTED_label_date_denver|$DAILY_label_date_denver
DAILY|LAG_DAYS|$DAILY_EXPECTED_LAG_DAYS|$DAILY_LAG_DAYS
DAILY|LEAD_DAYS|$DAILY_EXPECTED_LEAD_DAYS|$DAILY_LEAD_DAYS
DAILY_LAGLEAD|RUN_DATE|$DAILY_LAGLEAD_EXPECTED_RUN_DATE|$DAILY_LAGLEAD_RUN_DATE
DAILY_LAGLEAD|TIME_ZONE|$DAILY_LAGLEAD_EXPECTED_TIME_ZONE|$DAILY_LAGLEAD_TIME_ZONE
DAILY_LAGLEAD|START_DATE|$DAILY_LAGLEAD_EXPECTED_START_DATE|$DAILY_LAGLEAD_START_DATE
DAILY_LAGLEAD|END_DATE|$DAILY_LAGLEAD_EXPECTED_END_DATE|$DAILY_LAGLEAD_END_DATE
DAILY_LAGLEAD|START_DATE_TZ|$DAILY_LAGLEAD_EXPECTED_START_DATE_TZ|$DAILY_LAGLEAD_START_DATE_TZ
DAILY_LAGLEAD|END_DATE_TZ|$DAILY_LAGLEAD_EXPECTED_END_DATE_TZ|$DAILY_LAGLEAD_END_DATE_TZ
DAILY_LAGLEAD|CADENCE|$DAILY_LAGLEAD_EXPECTED_CADENCE|$DAILY_LAGLEAD_CADENCE
DAILY_LAGLEAD|label_date_denver|$DAILY_LAGLEAD_EXPECTED_label_date_denver|$DAILY_LAGLEAD_label_date_denver
DAILY_LAGLEAD|LAG_DAYS|$DAILY_LAGLEAD_EXPECTED_LAG_DAYS|$DAILY_LAGLEAD_LAG_DAYS
DAILY_LAGLEAD|LEAD_DAYS|$DAILY_LAGLEAD_EXPECTED_LEAD_DAYS|$DAILY_LAGLEAD_LEAD_DAYS
WEEKLY|RUN_DATE|$WEEKLY_EXPECTED_RUN_DATE|$WEEKLY_RUN_DATE
WEEKLY|TIME_ZONE|$WEEKLY_EXPECTED_TIME_ZONE|$WEEKLY_TIME_ZONE
WEEKLY|START_DATE|$WEEKLY_EXPECTED_START_DATE|$WEEKLY_START_DATE
WEEKLY|END_DATE|$WEEKLY_EXPECTED_END_DATE|$WEEKLY_END_DATE
WEEKLY|START_DATE_TZ|$WEEKLY_EXPECTED_START_DATE_TZ|$WEEKLY_START_DATE_TZ
WEEKLY|END_DATE_TZ|$WEEKLY_EXPECTED_END_DATE_TZ|$WEEKLY_END_DATE_TZ
WEEKLY|CADENCE|$WEEKLY_EXPECTED_CADENCE|$WEEKLY_CADENCE
WEEKLY|label_date_denver|$WEEKLY_EXPECTED_label_date_denver|$WEEKLY_label_date_denver
WEEKLY|LAG_DAYS|$WEEKLY_EXPECTED_LAG_DAYS|$WEEKLY_LAG_DAYS
WEEKLY|LEAD_DAYS|$WEEKLY_EXPECTED_LEAD_DAYS|$WEEKLY_LEAD_DAYS
MONTHLY|RUN_DATE|$MONTHLY_EXPECTED_RUN_DATE|$MONTHLY_RUN_DATE
MONTHLY|TIME_ZONE|$MONTHLY_EXPECTED_TIME_ZONE|$MONTHLY_TIME_ZONE
MONTHLY|START_DATE|$MONTHLY_EXPECTED_START_DATE|$MONTHLY_START_DATE
MONTHLY|END_DATE|$MONTHLY_EXPECTED_END_DATE|$MONTHLY_END_DATE
MONTHLY|START_DATE_TZ|$MONTHLY_EXPECTED_START_DATE_TZ|$MONTHLY_START_DATE_TZ
MONTHLY|END_DATE_TZ|$MONTHLY_EXPECTED_END_DATE_TZ|$MONTHLY_END_DATE_TZ
MONTHLY|CADENCE|$MONTHLY_EXPECTED_CADENCE|$MONTHLY_CADENCE
MONTHLY|label_date_denver|$MONTHLY_EXPECTED_label_date_denver|$MONTHLY_label_date_denver
MONTHLY|LAG_DAYS|$MONTHLY_EXPECTED_LAG_DAYS|$MONTHLY_LAG_DAYS
MONTHLY|LEAD_DAYS|$MONTHLY_EXPECTED_LEAD_DAYS|$MONTHLY_LEAD_DAYS
FISCAL_MONTHLY|RUN_DATE|$FISCAL_MONTHLY_EXPECTED_RUN_DATE|$FISCAL_MONTHLY_RUN_DATE
FISCAL_MONTHLY|TIME_ZONE|$FISCAL_MONTHLY_EXPECTED_TIME_ZONE|$FISCAL_MONTHLY_TIME_ZONE
FISCAL_MONTHLY|START_DATE|$FISCAL_MONTHLY_EXPECTED_START_DATE|$FISCAL_MONTHLY_START_DATE
FISCAL_MONTHLY|END_DATE|$FISCAL_MONTHLY_EXPECTED_END_DATE|$FISCAL_MONTHLY_END_DATE
FISCAL_MONTHLY|START_DATE_TZ|$FISCAL_MONTHLY_EXPECTED_START_DATE_TZ|$FISCAL_MONTHLY_START_DATE_TZ
FISCAL_MONTHLY|END_DATE_TZ|$FISCAL_MONTHLY_EXPECTED_END_DATE_TZ|$FISCAL_MONTHLY_END_DATE_TZ
FISCAL_MONTHLY|CADENCE|$FISCAL_MONTHLY_EXPECTED_CADENCE|$FISCAL_MONTHLY_CADENCE
FISCAL_MONTHLY|label_date_denver|$FISCAL_MONTHLY_EXPECTED_label_date_denver|$FISCAL_MONTHLY_label_date_denver
FISCAL_MONTHLY|LAG_DAYS|$FISCAL_MONTHLY_EXPECTED_LAG_DAYS|$FISCAL_MONTHLY_LAG_DAYS
FISCAL_MONTHLY|LEAD_DAYS|$FISCAL_MONTHLY_EXPECTED_LEAD_DAYS|$FISCAL_MONTHLY_LEAD_DAYS
" > "$today"_pd_test.txt

##########
echo -e "\n\n### The Process Dates Test Output Script has completed Successfully ###"
