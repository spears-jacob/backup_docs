PRIOR_MONTH_START_DATE=`date -d "$RUN_DATE -1 month" +%Y-%m-01`
PRIOR_MONTH_END_DATE=`date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days" +%Y-%m-%d`
echo $PRIOR_MONTH_START_DATE
echo $PRIOR_MONTH_END_DATE

hive -v -f src/update_dispositions.hql -hiveconf prior_month_start_date=$PRIOR_MONTH_START_DATE -hiveconf prior_month_end_date=$PRIOR_MONTH_END_DATE
