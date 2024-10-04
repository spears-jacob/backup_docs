#!/bin/bash
echo "Check data in hdfs"
dirs=(`hadoop fs -ls /incoming/app_figures_raw/* | awk '{print $NF}' | grep .gz$ | tr '\n' ' '`)

if [ ${#dirs[*]} -eq 0 ]; then

  echo "/incoming/app_figures_raw has no data " | mailx -s "AppFigures needs data" dl-pi-asp-reporting@charter.com;

else
  echo "Create directory in" ${data_sources_path}
  if [ ! -d "${data_sources_path}/appfigures/output" ]; then mkdir -p ${data_sources_path}/appfigures/output; fi

  echo "Clean up work space and download new data from hdfs"
  rm -rf ${data_sources_path}/appfigures/app_figures_raw/
  rm -rf ${data_sources_path}/appfigures/output

  #when trouble shoot need to copy data over, day by day from an edge node
  #hdfs dfs -cp /archive/app_figures/*2019-01-15 /incoming/app_figures_raw
  #hdfs dfs -cp /archive/app_figures/*2019-01-16 /incoming/app_figures_raw
  #hdfs dfs -cp /archive/app_figures/*2019-01-17 /incoming/app_figures_raw
  hdfs dfs -copyToLocal /incoming/app_figures_raw ${data_sources_path}/appfigures/

  dirs=(${data_sources_path}/appfigures/app_figures_raw/*)

  echo ${#dirs[@]}
  index=0
  while [ $index -lt ${#dirs[@]} ];
  do
    dir=${dirs[$index]}
    type=`echo ${dir//${data_sources_path}\/appfigures\/app_figures_raw} | cut -d '/' -f2 | sed 's/[-|0-9|_]//g'`

    echo $dir
    echo $type
    if [[ $type = 'afallreviews' ]];then

      bash bin/loop-jq.sh ${dir} ${data_sources_path}/appfigures/output/reviews reviews

    elif [[ $type = 'afappallsales' ]]; then

      formatted_date=$(sed -e 's/^\(.\{0\}\).*\(.\{10\}\)$/\2/' <<< $dir)
      bash bin/loop-jq.sh ${dir} ${data_sources_path}/appfigures/output/all_sales all_sales

      # further manipulate csv to put in missing date
      csvs=($(find ${data_sources_path}/appfigures/output/all_sales -type f -name '*.csv'))
      new_name=`echo -n ${csvs[*]} | sed 's/.csv//'`

      for csv in ${csvs[*]}
      do
        new_name=`echo -n $csv | sed 's/.csv//'`
        sed "s/$/,\"$formatted_date\"/" $csv > $new_name
        rm $csv
      done

    elif [[ $type = 'afappallussales' ]]; then

      formatted_date=$(sed -e 's/^\(.\{0\}\).*\(.\{10\}\)$/\2/' <<< $dir)
      bash bin/loop-jq.sh ${dir} ${data_sources_path}/appfigures/output/us_sales all_sales

      # further manipulate csv to put in missing date
      csvs=($(find ${data_sources_path}/appfigures/output/us_sales -type f -name '*.csv'))

      for csv in ${csvs[*]}
      do
        new_name=`echo -n $csv | sed 's/.csv//'`
        sed "s/$/,\"$formatted_date\"/" $csv > $new_name
        rm $csv
      done

    elif [[ $type = 'afappdailyranks' ]]; then

      bash bin/loop-jq.sh ${dir} ${data_sources_path}/appfigures/output/daily_ranks daily_ranks

    elif [[ $type = 'afappdailysales' ]]; then

      bash bin/loop-jq.sh ${dir} ${data_sources_path}/appfigures/output/daily_sales daily_sales

    elif [[ $type = 'afappdetails' ]]; then

      formatted_date=$(sed -e 's/^\(.\{0\}\).*\(.\{10\}\)$/\2/' <<< $dir)
      bash bin/loop-jq.sh ${dir} ${data_sources_path}/appfigures/output/app_details app_details

      # further manipulate csv to put in missing date
      csvs=($(find ${data_sources_path}/appfigures/output/app_details -type f -name '*.csv'))
      new_name=`echo -n ${csvs[*]} | sed 's/.csv//'`

      for csv in ${csvs[*]}
      do
        new_name=`echo -n $csv | sed 's/.csv//'`
        sed "s/$/,\"$formatted_date\"/" $csv > $new_name
        rm $csv
      done

    elif [[ $type = 'afappusdailysales' ]]; then

      bash bin/loop-jq.sh ${dir} ${data_sources_path}/appfigures/output/us_daily daily_sales

    elif [[ $type = 'afappratings' ]]; then

      bash bin/loop-jq.sh ${dir} ${data_sources_path}/appfigures/output/ratings ratings

    else
      echo $dir

    fi
    (( index++ ))
  done

  echo "Copy parsed files to hdfs"
  hdfs dfs -rm -r /apps/hive/warehouse/$TMP_db.db/app_figures_parsed/
  hdfs dfs -mkdir /apps/hive/warehouse/$TMP_db.db/app_figures_parsed/
  hdfs dfs -rm -r /apps/hive/warehouse/$TMP_db.db/app_figures_sentiment/
  hdfs dfs -mkdir /apps/hive/warehouse/$TMP_db.db/app_figures_sentiment/
  hdfs dfs -copyFromLocal ${data_sources_path}/appfigures/output/* /apps/hive/warehouse/$TMP_db.db/app_figures_parsed

fi

echo "Repair tables"
hive -f src/app_figure_repair.hql

echo "Clean up data source directory"
rm -rf ${data_sources_path}/appfigures/app_figures_raw/*
rm -rf ${data_sources_path}/appfigures/output

echo "Clean up work space and prepare for sentiment"
rm -rf ${data_sources_path}/appfigures/sentiment/
if [ ! -d "${data_sources_path}/appfigures/sentiment" ]; then mkdir -p ${data_sources_path}/appfigures/sentiment; fi
hive -f src/sentiment/sentiment.hql > ${data_sources_path}/appfigures/sentiment/reviews.tsv
hdfs dfs -copyFromLocal ${data_sources_path}/appfigures/sentiment/* /apps/hive/warehouse/$TMP_db.db/app_figures_sentiment
