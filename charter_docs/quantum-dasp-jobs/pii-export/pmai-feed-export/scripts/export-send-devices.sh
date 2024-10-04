export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=$ENVIRONMENT
export DASP_DB=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export SEC_db=${ENVIRONMENT}_sec_repo_sspp
export ARTIFACTS_PATH=$4
export SCALA_SCRIPT=$5
export HQL_FILE=$5
export SCRIPT_VERSION=$6


#!/bin/bash
extract_date_string=`date --date="$RUN_DATE" +%Y-%m-%d`
extract_date_string=`date --date="$extract_date_string -1 day" +%Y-%m-%d`

    echo "### Running export-send-devices.sh "
    export pmai_secure_location=s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/pmai_extracts/pmai_devices_exports/
    #export pmai_archive_files=s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/pmai_extracts/pmai_archive_files/
    export pmai_export_location=s3://com.charter.focus.prod.ingest.files/quantum/

    echo "### Files in the secured bucket '${pmai_secure_location}'"
    aws s3 ls $pmai_secure_location
    echo "################### : end list files in pmai secured bucket : ##################### "

    echo "### Files in the export bucket before copy '${pmai_export_location}'"
    aws s3 ls $pmai_export_location
    echo "################### : end list files in pmai bucket : ##################### "

    echo "### Move new files to  pmai export location ${pmai_export_location} "
    if [ "$ENVIRONMENT" == "prod" ]; then
      aws s3 mv $pmai_secure_location $pmai_export_location --recursive --acl bucket-owner-full-control
      echo "################### : end move local file to s3 bucket : ##################### "
    else
          echo "###  pmai file not moved to destination for the $ENVIRONMENT run ###"
    fi

    echo "### Files in the export bucket after copy'${pmai_export_location}'"
    aws s3 ls $pmai_export_location
    echo "################### : end list files in pmai_export_location bucket : ##################### "

    #echo "################### copy today's files to archive folder ################### "
    #aws s3 cp $pmai_secure_location $pmai_archive_files
    echo "###################  copy to archive finished ################### "
