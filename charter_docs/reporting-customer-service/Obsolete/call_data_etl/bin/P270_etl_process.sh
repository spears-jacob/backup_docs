echo "assigning processdate"
processdate=$(date --date="10 days ago" --rfc-3339=date)

#echo "echoing processdate"
echo $(processdate)

echo "running hive command"
#hive -hiveconf "processdate"="$(date --date="10 days ago" --rfc-3339=date)" -f ./src/P270_etl_process.hql
hive -hiveconf processdate=${processdate} -f ./src/P270_etl_process.hql
