#!/bin/bash
#
aws_region='us-east-1'

export TRUNCATE_TABLES=$1
export DELETE_START_DATE_HOUR_UTC=`echo $2 | sed 's/[-_]//g'`
export DELETE_END_DATE_HOUR_UTC=`echo $3 | sed 's/[-_]//g'`
export DELETE_START_DATE_UTC=`echo $2 | cut -c"1-10" | sed 's/[-_]//g'`
export DELETE_END_DATE_UTC=`echo $3 | cut -c"1-10" | sed 's/[-_]//g'`

remove_s3_data(){
    partition=$1
    echo "Truncating ${partition}"
    echo "Removing S3 data."
    echo --recursive "${partition}"
    
    if ! ( aws s3 rm --recursive "${partition}" )
    then
        echo "Failed to remove S3 data."
        continue
    fi
}

if [ -n "${TRUNCATE_TABLES}" ]
then
    for table in "${TRUNCATE_TABLES}"
    do
        echo "Checking for ${table}"
        db="`echo ${table} | cut -d '.' -f1`"
        db_table="`echo ${table} | cut -d '.' -f2`"
        if [ -z "${db}" ] || [ -z "${db_table}" ]
        then
            echo "Unable to parse db and table name from ${table}"
            continue
        fi
        echo "DB: ${db}"
        echo "Table: ${db_table}"
        S3_LOCATION=`aws glue get-table --database-name "${db}" --name "${db_table}" --region "${aws_region}" | jq -r '.Table.StorageDescriptor.Location'`
        echo "Getting partitions..."
        hive -e "SHOW PARTITIONS ${table}" | cut -d"/" -f1,2 | sed '1d' > S3_PARTITIONS.txt
        TMP_DIR=`mktemp -d`
        touch "${TMP_DIR}"/.truncated
        
        if [ -n "${S3_LOCATION}" ]
        then
            while read -r partition_date_hour
            do
                PARTITION_DATE_HOUR_UTC=`echo "${partition_date_hour}" | cut -d"/" -f2 | cut -d"=" -f2 | sed 's/[-_]//g'`
                
                if [ "${PARTITION_DATE_HOUR_UTC}" -ge "${DELETE_START_DATE_HOUR_UTC}" ] && [ "${PARTITION_DATE_HOUR_UTC}" -lt "${DELETE_END_DATE_HOUR_UTC}" ]
                then
                    echo "${table} location: ${S3_LOCATION}/${partition_date_hour}"
                    case "${S3_LOCATION}" in
                        s3://*)
                            remove_s3_data "${S3_LOCATION}/${partition_date_hour}"
                            
                            partition_date=`echo "${partition_date_hour}" | cut -d"/" -f"1"`
                            PARTITION_DATE_UTC=`echo "${partition_date}" | cut -d"=" -f2 | sed 's/[-_]//g'`
                            if [ "${PARTITION_DATE_UTC}" -gt "${DELETE_START_DATE_UTC}" ] && [ "${PARTITION_DATE_UTC}" -lt "${DELETE_END_DATE_UTC}" ]
                            then
                                remove_s3_data "${S3_LOCATION}/${partition_date}"
                            fi
                            ;;
                        *) echo "${table} location not an S3 URL: ${S3_LOCATION}" ;;
                    esac
                fi
            done < S3_PARTITIONS.txt
            
            echo "Removing partitions from glue metastore for ${db}.${db_table}"
            if ! (aws glue get-partitions --database-name=${db} --table-name=${db_table} --region=${aws_region} | jq -cr '[ { Values: .Partitions[].Values } ]' > ~/partitions.json
                  seq 0 25 1000 | xargs -I _ bash -c "cat ~/partitions.json | jq -c '.[_:_+25]'" | while read X; do aws glue batch-delete-partition --database-name=${db} --table-name=${db_table} --region=${aws_region} --partitions-to-delete=$X; done)
            then
              echo "Failed to update glue metastore."
            fi
            
            echo "Updating partitions."
            if ! hive -e "set hive.msck.path.validation=ignore; MSCK REPAIR TABLE \`${db}\`.\`${db_table}\`"
            then
                echo "Failed to update partitions."
            fi
            echo "Partitions updated."
        else
            echo "${table} location not found."
        fi
    done
fi
