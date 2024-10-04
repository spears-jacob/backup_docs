LOAD_DATE=`date -d "$RUN_DATE -10 day" +%Y-%m-%d` #since source data may change as far as 7 days back, this should update until then

echo "Running for "$RUN_DATE
echo "Which means loading data since "$LOAD_DATE

hive -v -f src/update_call_care_agg.hql -hiveconf load_date=$LOAD_DATE
