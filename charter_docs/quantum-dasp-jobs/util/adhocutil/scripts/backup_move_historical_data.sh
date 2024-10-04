# generic script for backing up historical data in prod and moving up the same named table from stg:
## Step 1 - copy data to historical in production
# prod credentials
gpc
tablename_to_use_for_definition="prod_dasp.asp_support_content_agg"

# get create table definition using Athena Utils (https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/tree/master/terminal/athena_utilities)
source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/athena_utilities/athena_utils.sh
execute_script_in_athena "show create table $tablename_to_use_for_definition;"
qr > show_create_table_complete
#clean up create table, remove first 16 header lines, remove backticks
perl -ni -e 'print unless $. < 16' show_create_table_complete
sed '/TBLPROPERTIES (/,$d' show_create_table_complete | perl -pe 's/\`//gi' > show_create_table
# set up variables for copy to historical location
export table_old_prod=$(grep "s3://" show_create_table | perl -pe "s|'$|/'|gi" )
export deploy_dt=$(date "+%Y%m%d")
echo  $deploy_dt
export table_historical_resting_location=$( echo $table_old_prod | perl -pe "s^prod_dasp^prod_dasp/historical^gi" | perl -pe "s^/'$^_$deploy_dt/'^gi"  )
# copy table to historical resting location
from=$(echo $table_old_prod| perl -pe "s/'//ig")
to=$(echo $table_historical_resting_location| perl -pe "s/'//ig")
echo "aws s3 cp --recursive $from $to"
aws s3 cp --recursive $from $to
# adjust create table to point to the historical location
export tablename_historical="$tablename_to_use_for_definition""_$deploy_dt"
perl -pi -e "s/CREATE EXTERNAL TABLE.+\($/CREATE EXTERNAL TABLE $tablename_historical \(/g" show_create_table
perl -pi -e "s^'s3://.+$^$table_historical_resting_location^g" show_create_table
cat show_create_table
# come up with create table hql for posterity and place it
historical_create_table_hql_name=$(echo $tablename_historical | perl -pe "s/^.+\.(.+)$/\1.sql/ig")
cat show_create_table > $historical_create_table_hql_name
historical_create_table_hql_location=$(echo $table_historical_resting_location | perl -pe "s|^'(.+historical/).+$|\1$historical_create_table_hql_name|ig")
cp_command="aws s3 --profile impulse-prod --region us-east-1 cp $historical_create_table_hql_name $historical_create_table_hql_location "
echo $cp_command
aws s3 --profile impulse-prod --region us-east-1 cp $historical_create_table_hql_name $historical_create_table_hql_location
xa " $(cat show_create_table) "
xa " MSCK REPAIR TABLE $tablename_historical "
qr
# then, check a few records to make sure the schema is correct in athena
# metric_agg: xa "select mso, visit_id, device_id, calls_within_24_hrs, technology_type from $tablename_historical where technology_type is not null limit 100"
# set_agg: xa "select label_date_denver, grain, metric_name, metric_value from $tablename_historical where metric_value <> 0 limit 100;"
rm show_create_table_complete
rm show_create_table
rm $historical_create_table_hql_name
## Step 2 - delete historical data from production, as it has been copied succesfully to the historical area
#first observe all your files are backed up:
aws s3 ls --recursive $to

## To Do: - add a comparison of objects between what is in historical location and what will be deleted ###
aws s3 --profile impulse-prod --region us-east-1 rm $from --recursive  --dryrun | wc -l
aws s3 ls --recursive $to | wc -l

# try the dryrun first, then delete without the dryrun flag, which makes it a 'real' delete action
aws s3 --profile impulse-prod --region us-east-1 rm $from --recursive  --dryrun
## Step 3 - copy stg data up to prod
export fromstg=$(echo $from | perl -pe "s/prod/stg/gi")
export toprod=$from
echo " aws s3 cp $fromstg $toprod --acl bucket-owner-full-control --recursive --profile impulse-prod"
aws s3 cp $fromstg $toprod --acl bucket-owner-full-control --recursive --profile impulse-prod
## Step 4 - Get code and environments up to date in AWS
# either run MDR metadata repair if the schema has not changed
# https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/tree/master/terminal/glue_metadata_repair
mdr $tablename_to_use_for_definition
#    OR
#    Drop the table
#    Merge dasp-jobs code if not already merged
#    build/plan/deploy latest DDL-manager with new table
#    then, build/plan/deploy all jobs that write to the table
#    next, repair the table in Athena
# then, check the table using the analagous Athena query, such as
# metric_agg: xa "select mso, visit_id, device_id, calls_within_24_hrs, technology_type from $tablename_to_use_for_definition where technology_type is not null limit 100"
# set_agg: xa "select label_date_denver, grain, metric_name, metric_value from $tablename_to_use_for_definition where metric_value <> 0 limit 100;"
# Finally, run the job(s) that populate the table for today in production to ensure they all work before the morning run
