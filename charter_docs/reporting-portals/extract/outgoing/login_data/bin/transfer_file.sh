#!/bin/bash
# This script is to make a tsv file and send it to a ftp folders

# 0. - Check for the existence of a RUN_DATE or bail out.
if [ "$RUN_DATE" != "" ]; then
  extract_run_date=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`
else
  echo "
    ### ERROR: Run date ($RUN_DATE) value not available
  " && exit 1
fi

LOCAL_DIR=login_data_feed_msa
TABLE_NAME=asp_extract_login_data_daily_pvt
HDFS_DIR=/archive/$TABLE_NAME

login_data_feed=$(hive -e "set hive.cli.print.header=false; USE $ENVIRONMENT; SELECT COUNT(date_denver) FROM $TABLE_NAME;" || echo "something failed when querying $TABLE_NAME"; exit 10001110101 ;)

echo "

    $login_data_feed records in $ENVIRONMENT.$TABLE_NAME

"

if [ "$login_data_feed" -lt 1 ]; then
  echo "Records not found in $ENVIRONMENT.$TABLE_NAME"
  exit 1
else
    echo $login_data_feed "Records found in $ENVIRONMENT.$TABLE_NAME"

    FILE_NAME="$TABLE_NAME"_${extract_run_date}_spa
    echo "Writing to $FILE_NAME"

    mkdir -p ${LOCAL_DIR}
    hive -e "set hive.cli.print.header=false;
             USE $ENVIRONMENT;
             SELECT
                    REGEXP_REPLACE(PROD.AES_DECRYPT256(account_number),'^0+','') as account_number,
                    max(myspectrum) as myspectrum,
                    max(specnet) as specnet,
                    max(spectrumbusiness) as spectrumbusiness,
                    max(specmobile) as specmoapp,
                    max(biller_type) as biller_type,
                    date_denver,
                    prod.aes_decrypt256(division) as division,
                    prod.aes_decrypt256(division_id) as division_id,
                    max(prod.aes_decrypt256(site_sys)) as sys,
                    max(prod.aes_decrypt256(prn)) as prn,
                    max(prod.aes_decrypt256(agn)) as agn,
                    max(prod.aes_decrypt256(acct_site_id)) as acct_site_id,
                    max(prod.aes_decrypt256(acct_company)) as acct_company,
                    max(prod.aes_decrypt256(acct_franchise)) as acct_franchise
             FROM $TABLE_NAME
             WHERE date_denver = '${extract_run_date}'
             and length(REGEXP_REPLACE(PROD.AES_DECRYPT256(account_number),'^0+','')) between 1 and 16
             group by date_denver,
                      REGEXP_REPLACE(PROD.AES_DECRYPT256(account_number),'^0+',''),
                      prod.aes_decrypt256(division),
                      prod.aes_decrypt256(division_id);
            " | sed 's/NULL//g' > ${LOCAL_DIR}/$FILE_NAME.tsv

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

        hdfs dfs -rm -skipTrash -r -f ${HDFS_DIR}/*.tgz

        hdfs dfs -moveFromLocal ${FILE_NAME}.tgz ${HDFS_DIR}
        hdfs dfs -ls ${HDFS_DIR}/${FILE_NAME}.tgz
    fi

else
    echo "job ended unsuccessfully -- Please re-run"
    exit 1
fi
