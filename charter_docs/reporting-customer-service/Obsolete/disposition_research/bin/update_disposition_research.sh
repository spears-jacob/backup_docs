PRIOR_DAY=`date -d "$RUN_DATE -1 day" +%Y-%m-%d`

echo $RUN_DATE
echo $PRIOR_DAY

hive -v -f src/update_disposition_research.hql -hiveconf prior_day=$PRIOR_DAY
