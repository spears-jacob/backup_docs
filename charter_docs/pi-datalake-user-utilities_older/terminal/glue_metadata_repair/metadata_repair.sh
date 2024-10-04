# metadata_repair.sh
# The shell script compares available metadata with populated folders in s3
# and adjust the metadata to match what is available.
#
# Make sure to have AWS credentials set beforehand
# prerequisites include having current AWS CLI, jq, and modern bash
# tested on a mac laptop and intended to be run as a script
# Also, update Athena output locations to match what is seen in Athena
# in the console (Query result location in Athena workgroup being used)
start_time=$(date +%s)

# Ensures that there is one input from the script execution
if [ $# -lt 1 ] || [ -z "$1" ] || [ ! "$1" ] || [[ ! "$1" = *.* ]]; then echo -e "\n\nUsage:\n\nsource metadata_repair.sh db.tablename\n\n"; kill -INT $$; fi

if [ -z "${BASH_VERSINFO}" ] || [ -z "${BASH_VERSINFO[0]}" ] || [ ${BASH_VERSINFO[0]} -lt 4 ]; then echo -e "\n\n\tThis script requires Bash version >= 4 \n\n\tPlease see https://dev.to/emcain/how-to-change-bash-versions-on-mac-with-homebrew-20o3\n\n"; kill -INT $$; fi

checkAWSversion=$(aws --version | perl -pe 's|^aws\-cli/(\d+\.\d+\.\d+) .+|$1|');
if [[ ! "$checkAWSversion" > "2.4.26" ]] ; then echo -e "\n\n\tPlease install the latest aws cli to use this script.\n\t\t https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html \n"; kill -INT $$; fi

aws sts get-caller-identity
# evaluate error code;  giving 0 on success and 255 if you have no credentials.
if [ $? -ne 0 ]; then echo -e "\n\tPlease try again with valid credentials and region"; kill -INT $$; fi

account=$(aws sts get-caller-identity --output json | jq -r ".Account")

# set these two ouput groups to match what is configured in the AWS Console Athena for each given workgroup
if [ $account -eq 213705006773 ]; then
  OUTPUT_S3_LOCATION="s3://213705006773-us-east-1-stg-workgroup-athena-results-bucket/pi-qtm-ssp/output/";
  echo -e "\n\n STG Impulse Dev Account and Athena configuration\n\n";
elif [ $account -eq 387455165365 ]; then
  OUTPUT_S3_LOCATION="s3://387455165365-us-east-1-prod-workgroup-athena-results-bucket/pi-qtm-ssp/output/";
  echo -e "\n\n PROD Impulse Account and Athena configuration\n\n";
else
  echo -e "\n\nUsage:\n\nsource metadata_repair.sh db.tablename\n\nUnknown Account and Athena configuration\n\n"; kill -INT $$;
fi

# Used for execution of AWS Athena compatible scripts via aws-cli
# @param {execution_script} - AWS Athena compatible script (required)
# Note: function will stop full process with 101 exit code if something went wrong
# (e.g. no permissions to execute query, query execution status 'FAILED')
execute_script_in_athena () {

    local execution_script=$1
    local submission_attempts=0

    echo -e "\n# Athena script: $execution_script\n"

    while [ ${submission_attempts} -lt 10 ]; do  #submit-query loop
      execution_id=$(aws athena start-query-execution \
                              --result-configuration "OutputLocation=$OUTPUT_S3_LOCATION" \
                              --query "QueryExecutionId" \
                              --output "text" \
                              --query-string "${execution_script}")
      if [ $? -ne 0 ]; then
        echo -e "\n\tWill try again in seven seconds\n\n";
        sleep 7;
        submission_attempts=$(expr $submission_attempts + 1)
      else submission_attempts=10;
      fi
    done  #submit-query loop

    echo "# Athena execution_id: $execution_id"
    echo ${execution_id} > execution_id # not in athena-utils

    local execution_status=$(get_athena_query_execution_status ${execution_id})
    echo "# Athena execution status: ${execution_status}"

    if [[ "${execution_status}" == "FAILED" ]];
    then
        finish_process_with_failure;
    fi
}

finish_process_with_failure (){
    echo -e '\n\n\tCheck the input database, table name, region, credentials and try agin.'
    cd $cwd
    kill -INT $$;
}

# Used for retrieving final state status (FAILED or SUCCEEDED) of AWS Athena Query via aws-cli
# @param {execution_id} - existing id of AWS Athena Query (required)
# Note: if query status is RUNNING get_athena_query_execution_status will be recursively executed
# until status be final (FAILED or SUCCEEDED)
get_athena_query_execution_status () {
    local execution_id=$1
    local status=$(aws athena get-query-execution --query-execution-id "${execution_id}" \
            --query "QueryExecution.Status.State" \
            --output "text"
            )
    # if status is RUNNING to wait query completion (status should be FAILED, or SUCCEEDED)
    if [[ ! "${status}" == "FAILED" ]] && [[ ! "${status}" == "SUCCEEDED" ]] ;
      then
          sleep 3
          get_athena_query_execution_status ${execution_id}
      else
          echo ${status}
    fi
}

export tablename_for_shoring_up_metadata=$1
db=$(echo $tablename_for_shoring_up_metadata | perl -pe 's/\..+$//');
t=$(echo $tablename_for_shoring_up_metadata | perl -pe 's/^.+\.(.+)$/$1/');

# second input parameter, if numeric, is used as the number of items ot limit the
# get-partitions call
if [[ $2 =~ ^[0-9]+$ ]]; then
  max_items=" --max-items $2 ";
  echo -e "\n\t Using $max_items in get-partitions call \n\n";
else unset max_items
fi

#folder housekeeping
export cwd=`pwd`
cd `mktemp -d`
export twd=`pwd`

## get s3 location
execute_script_in_athena " SHOW CREATE TABLE $tablename_for_shoring_up_metadata ; "
export s3exid="$(cat execution_id)";
## s3loc is parsed by grepping out the line that says s3 and removes single quotes
export s3loc=$(aws athena get-query-results --output json --query-execution-id $s3exid | jq -r ".ResultSet.Rows[].Data[].VarCharValue" | grep s3 | tr -d "'")
## treat slash at end by adding one if needed
slashpattern=".*/";  if ! [[ "$s3loc" =~ "$slashpattern" ]] ; then export s3loc="$s3loc/" ; fi
if ! [[ "$s3loc" =~ "s3://" ]]; then echo -e "\n\nUsage:\n\nsource metadata_repair.sh db.tablename\n\nEncountered issue with s3 location: $s3loc\n\n"; kill -INT $$; fi

echo -e "\n\n\t\-> S3 location of table: $s3loc\n\n"
echo $s3loc > s3Location

## get partitions
## pagination sometimes needed to get a full list of all partitions in the table
echo -e "\n\nNow getting partitions from glue for $tablename_for_shoring_up_metadata.\n\n"
start_time_partitions=$(date +%s)
agqr="aws glue get-partitions --output json --exclude-column-schema --database-name $db --table-name $t $max_items "
echo -e "now processing: $agqr "

unset NEXT_TOKEN

function parse_output() {
  if [ ! -z "$cli_output" ]; then
    echo $cli_output | jq -r ".Partitions[].StorageDescriptor.Location" >> listOfpartitionLocations.txt
    echo $cli_output | jq -r ".Partitions[].Values" >> listOfpartitionValues.txt
    echo $cli_output | jq -r '.Partitions[]  |  "\(.Values)" + .StorageDescriptor.Location' | perl -pe 's|[\["]||g' | perl -pe 's|[\]](?=s)|,|g' | perl -pe 's|[\]]$||g' >> listOfpartitionValuesANDLocations.txt
    echo $cli_output >> raw_partition_output
    NEXT_TOKEN=$(echo $cli_output | jq -r ".NextToken")
  fi
}

cli_output=$($agqr)
parse_output

while [ "$NEXT_TOKEN" != "null" ]; do
  if [ "$NEXT_TOKEN" == "null" ] || [ -z "$NEXT_TOKEN" ] ; then
    echo "now running   : $agqr "
    sleep 0.5
    cli_output=$($agqr)
    parse_output
  else
    echo "now paginating: $agqr --starting-token $NEXT_TOKEN"
    sleep 3
    cli_output=$($agqr --starting-token $NEXT_TOKEN)
    parse_output
  fi
done  #pagination loop
end_time_partitions=$(date +%s)
echo -e "Elapsed time for getting partitions: $(echo $end_time_partitions - $start_time_partitions | bc) seconds"

echo -e "\n\nNow getting folders in s3 with objects in them in the following location:\n\t$s3loc\n\n"
start_time_locations=$(date +%s)
aws s3 ls $s3loc --output text --recursive > all_objects
s3patternForZeroByteFiles="^\d{4}-\d{2}-\d{2} \d{2}:[0-5]\d:[0-5]\d\W+0\W"
egrep -h -v "$s3patternForZeroByteFiles" all_objects > all_files_and_intermediates
egrep -h -v ".hive-staging" all_files_and_intermediates > all_files
prefix=$(echo "$s3loc" | perl -pe 's/^\W*s3:\/{2}[^\/]+\///')
end_time_locations=$(date +%s)
echo -e "Elapsed time for getting locations: $(echo $end_time_locations - $start_time_locations | bc) seconds"



echo -e "\n\nNow sorting, deduplicating, and differentiating the two lists.\n\n"
# now parse the file names listing into a deduplicated, sorted file of populated folder locations
# from this:  2021-09-01 12:28:18   10769929 data/prod/atom_cs_call_care_data_3/call_end_date_east=2021-08-31/call_end_date_utc=2021-09-01/part-00001-f4fa5e8f-eb36-437f-b6d9-cecafc8a6a7e.c000"
# to this:    call_end_date_east=2021-08-31/call_end_date_utc=2021-09-01
perl -pe 's|^.+'$prefix'(.+)\/.+$|${1}|' all_files | perl -pe 's|^$||' > all_s3_locations.txt
sort -u all_s3_locations.txt > uniqueSortedlistOflocations.txt
awk 'NF' uniqueSortedlistOflocations.txt > l.list  #removes blank lines which causes spurious joins
location_count=$(cat l.list | wc -l)

# turn the list of partition locations into a list of partitions, then deduplicate and sort the file
replacementPattern="s|$(cat s3Location)||g";
perl -pe "$replacementPattern" listOfpartitionLocations.txt > listOfpartitions.txt
sort -u listOfpartitions.txt > uniqueSortedlistOfpartitions.txt
awk 'NF' uniqueSortedlistOfpartitions.txt > p.list  #removes blank lines which causes spurious joins
partition_count=$(cat p.list | wc -l)


# Any partition location that does not match its path is added to the spoiled partition list for deletion and recreation.
egrep -h "s3://" listOfpartitionValuesANDLocations.txt > partitions_with_locations
replacementPattern2="s|,$(cat s3Location)| ¥ |g";
cat partitions_with_locations | perl -pe "$replacementPattern2" | perl -pe 's|[^=/, ]+=|,|g' | perl -pe 's|/||g' | grep -E -v '(..*?). ¥ ,\1' > partitions_with_mismatched_locations
if [ -f  partitions_with_mismatched_locations ] && [ $(cat partitions_with_mismatched_locations | wc -l) -gt 0 ]; then
  cat partitions_with_mismatched_locations | perl -pe 's| ¥ ,||g' > spoiled_partitions_without_locations
fi

# Turn the list of spoiled partitions into the same format as the other two lists, if they exist.
# Spoiled partitions are those without locations in partition spec, leading to null pointer exceptions when querying.
# starts with preparing an array of the parition structure (p_structure),
# which is then used with the values from the spoiled parition list.
egrep -h -v "s3://" listOfpartitionValuesANDLocations.txt >> spoiled_partitions_without_locations

if [ -f spoiled_partitions_without_locations ] && [ $(cat spoiled_partitions_without_locations | wc -l) -gt 0 ]; then
  partition_example="$(tail -n 1 p.list)"; declare -a p_structure;
  IFS='/' read -ra p_struc_example <<< "$partition_example";
  for p_element_example in "${p_struc_example[@]}"; do
    element=$(echo "$p_element_example" | perl -pe 's|=.+$||g')
    p_structure+=($element); unset element;
  done

  # now treat the spoiled_partitions_without_locations with the partition structure
  while read -r spoiled; do
    unset spoiled_array; declare -a spoiled_array;
    IFS=',' read -ra spoiled_array <<< "$spoiled";

    for i in "${!spoiled_array[@]}"; do
      spoiled_line="${spoiled_line}${p_structure[$i]}=${spoiled_array[$i]}/"
    done
      spoiled_line=$(echo $spoiled_line | perl -pe 's|/$||g')
      echo $spoiled_line >> spoiled_partitions_list
      unset spoiled_line
  done < spoiled_partitions_without_locations
  sort -u spoiled_partitions_list > uniqueSortedlistOfSpoiledPartitions.txt
  awk 'NF' uniqueSortedlistOfSpoiledPartitions.txt > sp.list  #removes blank lines which causes spurious joins
fi

## Then, compare the two files, each way, with a deference to spoiled partitions showing up like new ones.
join -v 1 p.list l.list > ghost_partitions_not_in_s3
join -v 2 p.list l.list > nsp.list

if [ -f sp.list ] && [ $(cat sp.list | wc -l) -gt 0 ]; then
  join -v 1 nsp.list sp.list > new_partitions_in_s3_not_yet_in_glue
else cp nsp.list new_partitions_in_s3_not_yet_in_glue
fi

ghost_count=0
new_partition_count=0
spoiled_partition_count=0

## now go through each file and prepare to shore up metadata.
if [ -f ghost_partitions_not_in_s3 ] && [ $(cat ghost_partitions_not_in_s3 | wc -l) -gt 0 ]; then
  ghost_count=$(cat ghost_partitions_not_in_s3 | wc -l)
  perl -pe "s|=|='|g" ghost_partitions_not_in_s3 | perl -pe "s|/|', |g" | perl -pe "s|\n|'\n|g" > ghost_partition_completely_quoted
  perl -pe "s|^(.+)$|execute_script_in_athena \"ALTER TABLE $tablename_for_shoring_up_metadata DROP IF EXISTS PARTITION( \1 ); \" ; sleep 0.5 |g" ghost_partition_completely_quoted > drop_commands.sh
  cat drop_commands.sh

  perl -pe "s|[^=,]+=+||g" ghost_partition_completely_quoted | perl -pe "s|,|, |g" | perl -pe "s|'|\"|g" | perl -pe "s|'|\"|g" | perl -pe "s|^(.+)|{\"Values\": [ \1 ]}|g" > ghost_partitions_as_values_on_each_line
  split -l25 -a9 ghost_partitions_as_values_on_each_line bdp.
  for j in bdp.*; do
    perl -pe "s|\n|,|g" $j | perl -pe "s|,$||g" | perl -pe "s|^(.+)$|aws glue batch-delete-partition --database-name "$db" --table-name "$t" --output text --partitions-to-delete '[\1]'|g" > $j.sh
  done
  ls bdp.*.sh | perl -pe "s|^|source $twd/|g" > batch_delete_partitions_scripts.sh
fi

echo -e "\n\n"
if [ -f new_partitions_in_s3_not_yet_in_glue ] && [ $(cat new_partitions_in_s3_not_yet_in_glue | wc -l) -gt 0 ]; then
  new_partition_count=$(cat new_partitions_in_s3_not_yet_in_glue | wc -l)
  perl -pe "s|=|='|g" new_partitions_in_s3_not_yet_in_glue | perl -pe "s|/|', |g" | perl -pe "s|\n|'\n|g" > new_partitions_in_s3_not_yet_in_glue_completely_quoted
  perl -pe "s|^(.+)$|execute_script_in_athena \"ALTER TABLE $tablename_for_shoring_up_metadata ADD IF NOT EXISTS PARTITION( \1 ); \" ; sleep 0.5 |g" new_partitions_in_s3_not_yet_in_glue_completely_quoted > add_commands.sh
  cat add_commands.sh

  #perl -pe "s|[^=,]+=+||g" new_partitions_in_s3_not_yet_in_glue_completely_quoted | perl -pe "s|,|, |g" | perl -pe "s|'|\"|g" | perl -pe "s|'|\"|g" | perl -pe "s|^(.+)|{\"Values\": [ \1 ]}|g" > #new_partitions_in_s3_not_yet_in_glue_as_values_on_each_line
  #split -l99 -a9 new_partitions_in_s3_not_yet_in_glue_as_values_on_each_line bcp.
  #for i in bcp.*; do
  #  perl -pe "s|\n|,|g" $i | perl -pe "s|,$||g" | perl -pe "s|^(.+)$|aws glue batch-create-partition --database-name "$db" --table-name "$t" --output text --partition-input-list '[\1]'|g" > $i.sh
  #done
  #ls bcp.*.sh | perl -pe "s|^|source $twd/|g" > batch_create_partitions_scripts.sh
fi

if [ -f sp.list ] && [ $(cat sp.list | wc -l) -gt 0 ]; then
  spoiled_partition_count=$(cat sp.list | wc -l)

  perl -pe "s|=|='|g" sp.list | perl -pe "s|/|', |g" | perl -pe "s|\n|'\n|g" > renewal_commands_completely_quoted
  perl -pe "s|^(.+)$|execute_script_in_athena \"ALTER TABLE $tablename_for_shoring_up_metadata DROP IF EXISTS PARTITION( \1 ); \" ; sleep 0.5 \nexecute_script_in_athena \"ALTER TABLE $tablename_for_shoring_up_metadata ADD IF NOT EXISTS PARTITION( \1 ); \" ; sleep 0.5s \n|g" renewal_commands_completely_quoted > renew_commands.sh
  cat renew_commands.sh

  #while read -r sp_line; do
  #  values=$(echo $sp_line |  perl -pe 's|=|="|g' | perl -pe 's|/|", |g' | perl -pe 's|\n|"\n|g' | perl -pe 's|[^=, ]+=+||g' )
  #  echo " '[ {\"Values\": [ $values ],\"StorageDescriptor\" : {\"Location\": \"$(cat s3Location)$sp_line\"}}]' "
  #done < sp.list

  cat sp.list |  perl -pe 's|=|="|g' | perl -pe 's|/|", |g' | perl -pe 's|\n|"\n|g' | perl -pe "s|[^=, ]+=+||g" | perl -pe "s|^(.+)|{\"Values\": [ \1 ]}|g" > spoiled_partitions_as_values_on_each_line
    split -l25 -a9 spoiled_partitions_as_values_on_each_line bdsp.
  for j in bdsp.*; do
    perl -pe "s|\n|,|g" $j | perl -pe "s|,$||g" | perl -pe "s|^(.+)$|aws glue batch-delete-partition --database-name "$db" --table-name "$t" --output text --partitions-to-delete '[\1]'|g" > $j.sh
  done
  ls bdsp.*.sh | perl -pe "s|^|source $twd/|g" > batch_delete_spoiled_partitions_scripts.sh
fi

echo -e "
Regarding the table: $tablename_for_shoring_up_metadata
with its data here:$s3loc

--> There are \e[33m $partition_count \e[m partitions that are in glue
          and \e[33m $location_count \e[m in s3 with nonzero size data files.

--> The number of partitions that are in glue and not yet in s3 is the count
    of elements in ghost_partitions_not_in_s3: \e[31m   $ghost_count   \e[m

--> The number of partitions that are in glue and have missing or incorrect
    location specification in the parition is the count of
    elements in spoiled_partitions_without_locations: \e[36m   $spoiled_partition_count   \e[m

--> The number of partitions that are in s3 and not yet in the metadata is the count
    of elements in new_partitions_in_s3_not_yet_in_glue: \e[32m$new_partition_count   \e[m


Note: The list of partitions and list of files is still available
      in the following location if needed.

$twd/p.list
$twd/l.list

for ease of use, \$twd is aliased to $twd

"
if [ -f add_commands.sh ] || [ -f drop_commands.sh ] || [ -f renew_commands.sh ]; then
  echo -e "
Also Note: there are up to two shell scripts for use with Athena, one for dropping any
      ghost partitions and the other for adding any new paritions, available below and
      runnable from this terminal, as the 'execute_script_in_athena' function is available as
      long as the terminal is still open.  Replace '\e[33mcat\e[m' with '\e[33msource\e[m' below to run.

          "

  if [ -f drop_commands.sh ]; then echo -e "    cat "$twd/"\e[31mdrop_commands.sh  \e[m\n    cat "$twd/"\e[31mbatch_delete_partitions_scripts.sh  \e[m"; fi;
  if [ -f renew_commands.sh ];then echo -e "    cat "$twd/"\e[36mrenew_commands.sh  \e[m\n  "; fi;
  if [ -f add_commands.sh ];  then echo -e "    cat "$twd/"\e[32madd_commands.sh  \e[m\n    "; fi;
  echo -e "\n\n"
fi

#aws glue batch-create-partition --database-name stg --table-name atom_cs_call_care_data_3 --partition-input-list '[{"Values": [ "2020-10-02", "2020-10-06" ]}]' --output text
#aws glue batch-delete-partition --database-name stg_dasp --table-name asp_daily_report_data --partitions-to-delete '[{"Values": [ "2022-03-03" ]},{"Values": [ "2022-03-04" ]}]' --output text

#cleanup of athena execution_id, if present
if [ -f execution_id ]; then rm execution_id; fi

if [ $ghost_count -eq 0 ] && [ $new_partition_count -eq 0 ] && [ $spoiled_partition_count -eq 0 ]; then
echo -e "

 _ ||   _ _  _ _|_ _  _| _ _|_ _   . _     _ |. _|
(_|||  | | |(/_ | (_|(_|(_| | (_|  |_\  \/(_|||(_|

"

fi

end_time=$(date +%s)
echo "elapsed time: $(echo $end_time - $start_time | bc) seconds"

cd $cwd
