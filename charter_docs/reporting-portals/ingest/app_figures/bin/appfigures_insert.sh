#!/bin/bash
if [ $1 == 1 ];  then hdfs dfs -cp /incoming/app_figures_sentiment/* /apps/hive/warehouse/$TMP_db.db/app_figures_sentiment ; fi

echo "Load appfigures sentiment data to tmp table data"
hive -f src/app_figure_repair_sentiment.hql

echo "Load appfigures data"
hive -f src/app_figures.hql

echo "Load appfigures reviews/sentiment data"
if [ $1 == 1 ];  then hive -f src/app_figures_sentiment.hql ;
else hive -f src/app_figures_reviews_to_sentiment.hql  ;
fi

echo "Clean up data source directory"
rm -rf ${data_sources_path}/appfigures/sentiment
