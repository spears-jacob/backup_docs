# test for credentials, write permissions, input dates, tablename, s3 location, etc
if [ -z "${unq_id}" ] || [ -z "${s3_root}" ] || [ -z "${s3_path_till_partitions}" ] || [ -z "${tablename_to_use_for_definition}" ] || [ -z "${local_tablename}" ] || [ -z "${application_names_array}" ] || [ -z "${START_DATE}" ] || [ -z "${END_DATE}" ] || [ -z "${START_DATE_TZ}" ] || [ -z "${END_DATE_TZ}" ] ;
then echo -e "
  source pull_down_portion_of_table_to_local_hdfs.sh

    -- This script handles the movement of data from S3 to local HDFS.
    -- The creation, repair, and querying of the local table must be handled interactively
       or using a helper script.
    -- Do make sure to drop the local table after use, as it is of no use after
       the EMR spins down.
    -- The required environment variables for using this script are below
        - unq_id -- some sort of unique identifier to keep the resulting table name unique
        - s3_root -- the s3 bucket name itself
        - s3_path_till_partitions -- the bucket name, and all the folders up until the partitions
        - tablename_to_use_for_definition -- the table that uses the above path in its definition of location
        - local_tablename -- the name to use in the subsequent queries and drop table when the querying is done
        - application_names_array -- a space-delimited array of partitions to allow in the data
        - START_DATE, END_DATE -- start and end dates
        - START_DATE_TZ, END_DATE_TZ -- start and end dates with desired timezone-adjusted hour offset
          for limiting partitions

  This script requires several environment variables to be populated to run.
  Please examine the calling script and try again
  ";
kill -INT $$;
fi

export owd=`pwd`
cd `mktemp -d`

# get an array of days based on start and end environment variables
export loopdate=$START_DATE
declare -a dates_array
while [[ $loopdate < "$END_DATE" ]] || [[ $loopdate = "$END_DATE" ]]
do
  dates_array+=($loopdate)
  export loopdate=$(date -d "$loopdate + 1 day" +%F)
done

# then fetch all the objects for those days
for pdutc in ${dates_array[@]}
do
  echo $pdutc
  s3path="$s3_path_till_partitions/partition_date_utc=$pdutc/"
  aws s3 ls $s3path --recursive >> s3ls_all_objects
done

# then cut down to the application names
application_names_commas=$(IFS=, ; echo "${application_names_array[*]}")
application_names_egrep_pattern=${application_names_commas//,/\/\|}/
egrep "$application_names_egrep_pattern" s3ls_all_objects > s3ls_filtered_applications

# next, build the prefix file by testing the utc_date_hour
# from this:  2021-11-13 00:39:58    1838237 data/prod/core_quantum_events_app_parts/partition_date_utc=2021-11-12/partition_date_hour_utc=2021-11-12_22/visit__application_details__application_name=MySpectrum/8209422795b469559eca2107f200809e.orc
# to this:    s3://pi-qtm-global-prod-sspp/data/prod/core_quantum_events_app_parts/partition_date_utc=2021-11-12/partition_date_hour_utc=2021-11-12_22/visit__application_details__application_name=MySpectrum
while read line
do
  full_prefix=$(echo "$line" | perl -pe 's|^\d{4}-\d{2}-\d{2} \d{2}:[0-5]\d:[0-5]\d\W+\d+\W(.+)\/.+$|${1}|')
  utc_hour=$(echo "$line" | perl -pe 's|^.+partition_date_hour_utc=(\d{4}-\d{2}-\d{2}_\d{2})\/.+$|${1}|')
  if [[ "${utc_hour}" = "${START_DATE_TZ}" ]] || [[ "${utc_hour}" > "${START_DATE_TZ}" ]] && [[ "${utc_hour}" < "${END_DATE_TZ}" ]]; then
    s3prefix="$s3_root/$full_prefix"
    echo $s3prefix >> list_of_prefixes_with_duplicates
  fi
done < s3ls_filtered_applications

cat list_of_prefixes_with_duplicates | sort -u > list_of_unique_prefixes

# this copies only actual files, as the placeholder_$folder$ objects corrupt the table metadata
# it takes a minute for a day of cqe sspp data, and 5 minutes to copy a month...
s3-dist-cp --src "$s3_path_till_partitions" --dest /tmp/$local_tablename/ --srcPrefixesFile file:///${PWD}/list_of_unique_prefixes --srcPattern=.*.orc

# please refrain from copying more than 50% of the DFS capacity, as noted below
# SSPP data, by in large, is about 1GB/day, and since the current standard pushbutton
# cluster has about 120G capacity, one month would take about 25% capacity, and two months 50%, etc
hdfs dfsadmin -report

cd $owd
unset owd

# hdfs dfs -rm -R -skipTrash /tmp/$local_tablename/
