TWO_DAYS=`date -d "$RUN_DATE -2 day" +%Y-%m-%d`
THIRTYONE_DAYS=`date -d "$RUN_DATE -31 day" +%Y-%m-%d`

echo $RUN_DATE
echo $TWO_DAYS
echo $THIRTYONE_DAYS

hive -f src/daily_cs_qc.hql -hiveconf two_days=$TWO_DAYS -hiveconf thirtyone_days=$THIRTYONE_DAYS
