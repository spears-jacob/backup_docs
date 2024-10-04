LOAD_DATE=`date -d "$RUN_DATE -10 day" +%Y-%m-%d` #source data isn't updated until 1600 Mountain Time
#So this load date is only useful if this flow is scheduled after that point

echo "Running for "$RUN_DATE
echo "Which means loading data since "$LOAD_DATE

hive -v -f src/update_cwpv_agg.hql -hiveconf load_date=$LOAD_DATE
