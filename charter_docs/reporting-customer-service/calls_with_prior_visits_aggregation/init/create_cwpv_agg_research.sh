PRIOR_DAY=`date -d "$RUN_DATE -1 day" +%Y-%m-%d`

echo "Running for "$RUN_DATE
echo "Which means loading data up to and including "$PRIOR_DAY

hive -v -f init/create_cwpv_agg_research.hql -hiveconf prior_day=$PRIOR_DAY
