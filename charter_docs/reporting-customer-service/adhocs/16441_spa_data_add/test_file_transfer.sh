LOCAL_DIR=login_data_feed_msa
TABLE_NAME=asp_extract_login_data_daily_pvt
HDFS_DIR=/archive/$TABLE_NAME
ENVIRONMENT=dev
extract_run_date=`date --date="yesterday" +%Y-%m-%d`

echo $extract_run_date
echo $ENVIRONMENT

login_data_feed=$(hive -e "set hive.cli.print.header=false; USE $ENVIRONMENT; SELECT COUNT(date_denver) FROM $TABLE_NAME;" || echo "something failed when querying $TABLE_NAME"; exit 10001110101 ;)

echo "

    $login_data_feed records in $ENVIRONMENT.$TABLE_NAME

"

if [ "$login_data_feed" -lt 1 ]; then
  echo "Records not found in $ENVIRONMENT.$TABLE_NAME"
  exit 1
else
    echo $login_data_feed "Records found in $ENVIRONMENT.$TABLE_NAME"

    FILE_NAME="$TABLE_NAME"_${extract_run_date}_test

    mkdir -p ${LOCAL_DIR}
    hive -e "set hive.cli.print.header=true;
             USE $ENVIRONMENT;
             SELECT PROD.AES_DECRYPT(account_number) as account_number,
                    myspectrum,
                    specnet,
                    spectrumbusiness,
                    date_denver,
                    division_id,
                    site_sys,
                    prn,
                    agn
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

        hdfs dfs -rm -skipTrash -r -f ${HDFS_DIR}/*.tgz

        hdfs dfs -moveFromLocal ${FILE_NAME}.tgz ${HDFS_DIR}
        hdfs dfs -ls ${HDFS_DIR}/${FILE_NAME}.tgz
    fi
else
    echo "job ended unsuccessfully -- Please re-run"
    exit 1
fi

