#!/bin/bash

export data_sources_path=$1
export warehouse_path=$2
export TMP_db=$3
export incoming_path=$4
export SCRIPT_VERSION=$5
export DaysToLag=$6
export s3_bucket=$7

echo "Check data in hdfs"
dirs=(`hadoop fs -ls -R ${incoming_path}/* | awk '{print $NF}' | grep .gz$ | tr '\n' ' '`)

if [ ${#dirs[*]} -eq 0 ]; then

  echo "${incoming_path} has no data " | mailx -s "AppFigures needs data" dl-pi-asp-reporting@charter.com;

else
  echo "Create directory in" ${data_sources_path}
  if [ ! -d "${data_sources_path}/appfigures/app_figures_raw" ]; then mkdir -p ${data_sources_path}/appfigures/app_figures_raw; fi

  echo "Clean up work space and download new data from hdfs"
  rm -rf ${data_sources_path}/appfigures/app_figures_raw/*
  rm -rf ${data_sources_path}/appfigures/output/*


  # based on DaysToLag value source files are moved to archive dir
  # TODO: that part can be refactored to remove the step for debugging and keep only recursive copying of folders
  extract_run_date=`date --date="${RUN_DATE} -${DaysToLag} day" +%Y-%m-%d`
  echo "Move source files for $extract_run_date from ${incoming_path} to ${incoming_path}/archive/${extract_run_date}"
  # get list of *gz files from s3
  aws s3 ls ${incoming_path} --human-readable --recursive | grep -v "archive" | while read line ; do
    # get full path to file and relative path
    file=$(echo ${line} | awk '{print $5}')
    relative_path=$(echo `dirname $file`)
    # generating destination path based on relative path
    relative_dest_path=$(echo $relative_path | sed "s/nifi\/app_figures\//nifi\/app_figures\/archive\/${extract_run_date}\//g")
    # can be used for debugging. that iteration moves *gz files for specified date based on DaysToLag value 
    if [ ${DaysToLag} -ne 0 ]; then
      creation_date=$(echo ${line} | awk '{print $1}')
      if [[ ${creation_date} == *"${extract_run_date}"* ]]; then
        aws s3 mv ${s3_bucket}/${relative_path} ${s3_bucket}/${relative_dest_path} --recursive --acl bucket-owner-full-control || true
      fi
    # that step moves all listed *gz files from root app_figures dir to archive folder if there is no DaysToLag value 
    else
      if [[ ${relative_path} == *"app_figures"* ]]; then
        aws s3 mv ${s3_bucket}/${relative_path} ${s3_bucket}/${relative_dest_path} --recursive --acl bucket-owner-full-control || true
      fi
    fi
    # there is moving with --acl bucket-owner-full-control parameter to avoid an access ussue between stg and prod envs
  done

  # copy source files from archive dir to local dir
  hdfs dfs -copyToLocal ${incoming_path}/archive/${extract_run_date}/* ${data_sources_path}/appfigures/app_figures_raw/

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

      bash ./scripts/loop-jq-${SCRIPT_VERSION}.sh ${dir} ${data_sources_path}/appfigures/output/reviews reviews

    elif [[ $type = 'afappallsales' ]]; then

      formatted_date=$(sed -e 's/^\(.\{0\}\).*\(.\{10\}\)$/\2/' <<< $dir)
      bash ./scripts/loop-jq-${SCRIPT_VERSION}.sh ${dir} ${data_sources_path}/appfigures/output/all_sales all_sales

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
      bash ./scripts/loop-jq-${SCRIPT_VERSION}.sh ${dir} ${data_sources_path}/appfigures/output/us_sales all_sales

      # further manipulate csv to put in missing date
      csvs=($(find ${data_sources_path}/appfigures/output/us_sales -type f -name '*.csv'))

      for csv in ${csvs[*]}
      do
        new_name=`echo -n $csv | sed 's/.csv//'`
        sed "s/$/,\"$formatted_date\"/" $csv > $new_name
        rm $csv
      done

    elif [[ $type = 'afappdailyranks' ]]; then

      bash ./scripts/loop-jq-${SCRIPT_VERSION}.sh ${dir} ${data_sources_path}/appfigures/output/daily_ranks daily_ranks

    elif [[ $type = 'afappdailysales' ]]; then

      bash ./scripts/loop-jq-${SCRIPT_VERSION}.sh ${dir} ${data_sources_path}/appfigures/output/daily_sales daily_sales

    elif [[ $type = 'afappdetails' ]]; then

      formatted_date=$(sed -e 's/^\(.\{0\}\).*\(.\{10\}\)$/\2/' <<< $dir)
      bash ./scripts/loop-jq-${SCRIPT_VERSION}.sh ${dir} ${data_sources_path}/appfigures/output/app_details app_details

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

      bash ./scripts/loop-jq-${SCRIPT_VERSION}.sh ${dir} ${data_sources_path}/appfigures/output/us_daily daily_sales

    elif [[ $type = 'afappratings' ]]; then

      bash ./scripts/loop-jq-${SCRIPT_VERSION}.sh ${dir} ${data_sources_path}/appfigures/output/ratings ratings

    else
      echo $dir

    fi
    (( index++ ))
  done

  echo "Copy parsed files to hdfs"
  hdfs dfs -rm -r ${warehouse_path}/$TMP_db.db/app_figures_parsed/
  hdfs dfs -mkdir -p ${warehouse_path}/$TMP_db.db/app_figures_parsed/
  # run_sentiment always is '0'... that step is deprecated
  # hdfs dfs -rm -r ${warehouse_path}/$TMP_db.db/app_figures_sentiment/
  # hdfs dfs -mkdir -p ${warehouse_path}/$TMP_db.db/app_figures_sentiment/
  hdfs dfs -copyFromLocal ${data_sources_path}/appfigures/output/* ${warehouse_path}/$TMP_db.db/app_figures_parsed

fi

echo "Repair tables"
hive -f ${ARTIFACTS_PATH}/hql/app_figure_repair-${SCRIPT_VERSION}.hql

# run_sentiment always is '0'... that step is deprecated
# echo "Clean up work space and prepare for sentiment"
# rm -rf ${data_sources_path}/appfigures/sentiment/
# if [ ! -d "${data_sources_path}/appfigures/sentiment" ]; then mkdir -p ${data_sources_path}/appfigures/sentiment; fi
# hive -f ${ARTIFACTS_PATH}/hql/sentiment-${SCRIPT_VERSION}.hql > ${data_sources_path}/appfigures/sentiment/reviews.tsv
# hdfs dfs -copyFromLocal ${data_sources_path}/appfigures/sentiment/* ${warehouse_path}/$TMP_db.db/app_figures_sentiment


if [ $? -eq 0 ]; then
  echo "### SUCCESS"
  echo "Clean up data source directory"
  rm -rf ${data_sources_path}/appfigures/app_figures_raw/*
  rm -rf ${data_sources_path}/appfigures/output
else
  echo "### ERROR: Job ended unsuccessfully -- Please re-run" && exit 1
fi
