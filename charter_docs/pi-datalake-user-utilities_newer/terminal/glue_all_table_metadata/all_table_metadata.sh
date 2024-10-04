# Make sure to have AWS credentials set beforehand

# check prerequisites and credentials found in common_utilities needs to be loaded already to use this script, preferably in ~/.bash_profile
c_preq
c_creds

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
