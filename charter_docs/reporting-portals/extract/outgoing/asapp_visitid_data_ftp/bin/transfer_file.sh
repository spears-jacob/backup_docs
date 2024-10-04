#!/bin/bash
# This script is to make a tsv file and send it to a ftp folders

# 0. - Check for the existence of a RUN_DATE or bail out.
export RUN_DATE="$1"
echo "RUN_DATE: " "${RUN_DATE}"

##export DaysToLag_TMP="${2}"
export DaysToLag_TMP=${VAR01}
echo "DaysToLag is " ${DaysToLag_TMP}

export DaysToLag=`expr ${DaysToLag_TMP} - 1`

if [ "$RUN_DATE" != "" ]; then
  extract_run_date=`date --date="$RUN_DATE -$DaysToLag day" +%Y-%m-%d`
else
  echo "
    ### ERROR: Run date ($RUN_DATE) value not available
  " && exit 1
fi

LOCAL_DIR=asapp_visitid_data_feed
TABLE_NAME=asp_extract_asapp_visitid_data_daily_pvt
HDFS_DIR=/archive_secure/$TABLE_NAME

visitid_data_feed=$(hive -e "set hive.cli.print.header=false; USE $ENVIRONMENT; SELECT COUNT(date_denver) FROM $TABLE_NAME;" || echo "something failed when querying $TABLE_NAME"; exit 10001110101 ;)

echo "

    $visitid_data_feed records in $ENVIRONMENT.$TABLE_NAME

"

if [ "$visitid_data_feed" -lt 1 ]; then
  echo "Records not found in $ENVIRONMENT.$TABLE_NAME"
  exit 1
else
    echo $visitid_data_feed "Records found in $ENVIRONMENT.$TABLE_NAME"

    FILE_NAME="$TABLE_NAME"_${extract_run_date}_spa
    echo "Writing to $FILE_NAME"

    mkdir -p ${LOCAL_DIR}
    hive -e "set hive.cli.print.header=true;
             USE $ENVIRONMENT;
             SELECT external_session_id,
                    application_name,
                    prod.aes_decrypt256(account_number) as account_number,
                    biller_type,
                    date_denver,
                    prod.aes_decrypt256(division) as division,
                    prod.aes_decrypt256(division_id) as division_id,
                    prod.aes_decrypt256(sys) as sys,
                    prod.aes_decrypt256(prn) as prn,
                    prod.aes_decrypt256(agn) as agn,
                    prod.aes_decrypt256(acct_site_id) as acct_site_id,
                    prod.aes_decrypt256(acct_company) as acct_company,
                    prod.aes_decrypt256(acct_franchise) as acct_franchise
             FROM $TABLE_NAME
             WHERE date_denver = '${extract_run_date}';
            " > ${LOCAL_DIR}/$FILE_NAME.tsv
fi

if [ $? -eq 0 ]; then

    ldr=$(pwd)/${LOCAL_DIR}

    echo "report generated at $ldr"
    if [ "$ENVIRONMENT" != "prod" ]; then
        echo "Not prod; no file transfer attempted"
      else
        cd $ldr
        tar --force-local --warning=no-file-changed -czvf $FILE_NAME.tgz  $FILE_NAME.tsv --remove-files|| [[ $? -eq 1 ]]
        echo "
             moving file to hdfs after clearing
        "
        hdfs dfs -mkdir -p ${HDFS_DIR}

        ##hdfs dfs -rm -skipTrash -r -f ${HDFS_DIR}/*.tgz

        hdfs dfs -moveFromLocal ${FILE_NAME}.tgz ${HDFS_DIR}
        hdfs dfs -ls ${HDFS_DIR}/${FILE_NAME}.tgz
    fi

else
    echo "job ended unsuccessfully -- Please re-run"
    exit 1
fi
