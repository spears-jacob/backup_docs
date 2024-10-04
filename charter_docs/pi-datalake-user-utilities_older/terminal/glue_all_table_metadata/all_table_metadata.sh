# Make sure to have AWS credentials set beforehand
# prerequisites include having AWS CLI and jq
# tested on a mac laptop and intended to be run interactively

aws sts get-caller-identity
# evaluate error code;  giving 0 on success and 255 if you have no credentials.
if [ $? -ne 0 ]; then echo -e "\n\tPlease try again with valid credentials and region"; kill -INT $$; fi


#folder housekeeping
export cwd=`pwd`
cd `mktemp -d`
export twd=`pwd`

## get a list of all databases in the given region / account
aws glue get-databases --output json > databases.json
jq -r '.DatabaseList [] | "\(.Name)"' databases.json > databases.txt
readarray -t database_array < databases.txt
echo ${database_array[@]}

## now loop through the above array - this will take a few minutes
for db in "${database_array[@]}"
do
  echo -e "--> $(date) --√ Now processing $db "

  # gets all the tables, filters them down to relevant fields, and adds them to a single file (all_tables)
  aws glue get-tables --output json --database-name $db > $db.json
  jq -r '.TableList[] | "\(.Name), \(.UpdateTime), \(.StorageDescriptor.Location), \(.CreatedBy)"' $db.json > $db.tables
  sed -i -e "s/^/$db./" $db.tables #adds the database prefix
  cat $db.tables >> all_tables
  sleep 0.5
done

echo "Explore all_tables for a point-in-time view of all available tables using the current credentials and region."

cd $cwd

echo "
Usage:
       source all_table_metadata.sh

  Then, with all_tables metadata available, make use of it, such as by performing the following.

       grep <table_name_pattern> $twd/all_tables
  or   less $twd/all_tables
"
