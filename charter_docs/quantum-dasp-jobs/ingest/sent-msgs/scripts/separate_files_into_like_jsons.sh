#!/bin/bash

#updates cluster to latest software, quietly
sudo -s yum -y -t -q update

export RAW_FILE_LOCATION="s3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/proactive_maintenance/"
export RAW_FILE_DAILY_LOCATION="${RAW_FILE_LOCATION}partition_date=${START_DATE}/"
# future-state:  export RAW_FILE_LOCATION="s3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/sent_messages/incoming/"
export SEPARATED_FILE_LOCATION="s3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/sent_messages/processed"
export PROBLEMATIC_FILES_LOCATION="s3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/sent_messages/problematic"

export HDFS_LOCATION=rawfiles_${START_DATE}
export LOCAL_FOLDER=sent_messages_${START_DATE}

# pull in the raw mixed-up schema files, compress them, and make up to 2gb sized files --
# this takes 35-40 min for ~30gb per day, up to 4 retries
dtecho "### Pulling mixed raw files down for 30-40 min for ~30gb of files in a day ..................."
export s3distcp_attempt=1;
export is_s3distcp_successful=0;
export num_retries=4;
while  [ "${s3distcp_attempt}" -le "${num_retries}" ] && [ "${is_s3distcp_successful}" -ne 1 ] ;
do
  hdfs dfs -rm -r -f -skipTrash "/${HDFS_LOCATION}"
  s3-dist-cp --src ${RAW_FILE_DAILY_LOCATION} --dest hdfs:///${HDFS_LOCATION} --outputCodec=gz --targetSize=4096 --groupBy='.*/(partition_date=\d{4}-\d{2}-\d{2}/partition_hour=\d{2}/).*.avro'
  if [ $? -ne 0 ] ; then  echo -e "\n\ts3-dist-cp had an issue - attempt #$s3distcp_attempt of ${num_retries} "; sleep 30; else export is_s3distcp_successful=1; fi
  ((s3distcp_attempt++));
done

# go to area that uses mounted EBS (100+GB) rather than root volume (10GB)
dtecho "### Processing raw files - pulling them into local FS from Hadoop FS..................."
cd /mnt; mkdir ${LOCAL_FOLDER}; cd ${LOCAL_FOLDER}; mkdir mixedupschema; mkdir epid; mkdir hp; cd mixedupschema;

# copy from hdfs to local
hadoop fs -copyToLocal  /${HDFS_LOCATION}/*/* .  || { echo "hadoop fs -copyToLocal failure"; exit 101; }

dtecho "### Processing raw files - separate them using zgrep ..................."
# separates different schemas (event post id and header/payload) by grepping the gzipped files
export zgrepsuccess=true
zgrep --no-filename '^{"eventPostId":' *.gz > ../epid/${START_DATE}.epid.json; if [ $? -ne 0 ] ; then  dtecho "zgrep had an issue with epid;"; export zgrepsuccess=false; fi
zgrep --no-filename '^{"header":' *.gz > ../hp/${START_DATE}.hp.json; if [ $? -ne 0 ] ; then  dtecho "zgrep had an issue with hp;"; export zgrepsuccess=false; fi

# in the rare event that zgrep does not work, unzip files and grep the uncompressed ones
if [ ${zgrepsuccess} == "false" ] ; then
  dtecho "###Processing raw files using plain grep after uncompressing them, as this time zgrep did not work as expected ..................."
  rm -f ../epid/${START_DATE}.epid.json
  rm -f ../hp/${START_DATE}.hp.json;
  gunzip *.gz
  grep --no-filename '^{"eventPostId":' * > ../epid/${START_DATE}.epid.json; if [ $? -ne 0 ] ; then  dtecho "grep had an issue with the uncompressed files;";  fi
  grep --no-filename '^{"header":' * > ../hp/${START_DATE}.hp.json; if [ $? -ne 0 ] ; then  dtecho "grep had an issue with the uncompressed files;"; fi
fi

# show the size of the files for the day
cd ..; du -sh *;

# possibly todo
# now subdivide the hp partition date by the .[].comm.startDatetime
# first step, get a unique list of all the dates in the hp file
# jq -r '.[].comm.startDatetime' hp.json | perl -ple"s/^(\d\d\d\d-\d\d-\d\d).+/\$1/" | perl -ple"s/^null//" | sort -u
# grep \"startDatetime\":\"2022-08-01 hp.json | wc -l

# copy up the organized json text files to s3
dtecho "### Placing processed files back to s3 secure ..................."
dtecho "aws s3 cp epid/${START_DATE}.epid.json ${SEPARATED_FILE_LOCATION}/epid/partition_date=${START_DATE}/"
aws s3 cp --no-progress epid/${START_DATE}.epid.json ${SEPARATED_FILE_LOCATION}/epid/partition_date=${START_DATE}/ || { echo "aws s3 cp failure"; exit 101; }

dtecho "aws s3 cp hp/${START_DATE}.hp.json ${SEPARATED_FILE_LOCATION}/hp/partition_date=${START_DATE}/"
aws s3 cp --no-progress hp/${START_DATE}.hp.json ${SEPARATED_FILE_LOCATION}/hp/partition_date=${START_DATE}/ || { echo "aws s3 cp failure"; exit 101; }

dtecho "### Listing processed files from s3 secure ..................."
aws s3 ls ${SEPARATED_FILE_LOCATION}/epid/partition_date=${START_DATE}/
aws s3 ls ${SEPARATED_FILE_LOCATION}/hp/partition_date=${START_DATE}/
cd ~

dtecho "###Cleaning up /mnt/${LOCAL_FOLDER} ..................."
rm -r /mnt/${LOCAL_FOLDER} || { echo "rm -r /mnt/${LOCAL_FOLDER} failure"; exit 101; }

dtecho "###Cleaning up HDFS location: /${HDFS_LOCATION} ..................."
hdfs dfs -rm -r  -skipTrash "/${HDFS_LOCATION}"

dtecho "###Adding Partitions to raw tables: ..................."
execute_script_in_athena "ALTER TABLE ${SEC_db}.asp_sentmsgs_hp_raw   ADD IF NOT EXISTS PARTITION(partition_date='${START_DATE}');"
execute_script_in_athena "ALTER TABLE ${SEC_db}.asp_sentmsgs_epid_raw ADD IF NOT EXISTS PARTITION(partition_date='${START_DATE}');"

dtecho "### Finished processessing files ..................."