# script for backing up historical data in prod and moving up the same named table from stg
# get prod credentials in the current terminal, common_utilities are req'd, load Athena Utils, and specify the table to archive and the step
#
# Usage: source move_historical_data.sh;
#   then run the function as needed using steps 1-4 as required.
#      mhd <db>.<tablename_in_prod_to_archive> <step_number>
##           Step 1 - copy data to historical folder in production
##           Step 2 - prepares historical table ddl, creates and repairs historical table
##           Step 3 - delete data from production table
##           Step 4 - move data up from stg environment

# move historical data
function mhd() {
  if [[ $# -ne 2 ]] ; then echo -e "\n\n${On_Blue}Usage: mhd <db>.<tablename_in_prod_to_archive> <step_number>${NC}\n\n\tStep 1 - copy data to historical folder in production\n\tStep 2 - prepares historical table ddl, creates and repairs historical table\n\tStep 3 - delete data from production table\n\tStep 4 - move data up from stg environment\n"; kill -INT $$; fi
  tablename_to_use_for_definition="${1}"
  fn_exists execute_script_in_athena; if [ $? -ne 0 ]; then  echo -e "\n${On_Red}\t\n\tPlease load Athena utilities in order to use${IBlue} mhd, ${On_Red}also found in the pi-datalake-user-utilities repository.${NC}\n"; kill -INT $$ ; fi
  export cwd=`pwd`; cd `mktemp -d`; export twd=`pwd`;

  # get create table definition using
  execute_script_in_athena "show create table $tablename_to_use_for_definition;"; get_athena_results > show_create_table_complete
  if [ $? -ne 0 ]; then cd $cwd; echo -e "\n${On_Red}\tPlease make sure the tablename ($tablename_to_use_for_definition) is valid for the credentials currently being used.${NC}\n"; kill -INT $$; fi

  #clean up create table, remove first 16 header lines, remove backticks
  perl -ni -e 'print unless $. < 16' show_create_table_complete;
  sed '/TBLPROPERTIES (/,$d' show_create_table_complete | perl -pe 's/\`//gi' > show_create_table

  # set up variables for copy to historical location
  local table_old_prod=$(grep "s3://" show_create_table | perl -pe "s|'$|/'|gi" );
  local deploy_dt=$(date "+%Y%m%d");
  local table_historical_resting_location=$(echo $table_old_prod | perl -pe "s^/(prod_\w+)^/\1/historical^gi" | perl -pe "s^/'$^_$deploy_dt/'^gi"  )
  local from=$(echo $table_old_prod| perl -pe "s/'//ig"); to=$(echo $table_historical_resting_location| perl -pe "s/'//ig");

  if   [[ ${2} -eq 1 ]] ; then
    echo -e "\n\n${On_IWhite}## mhd ## Step 1 - copy data to historical folder${NC}\n"
    move_statement="${BICyan}aws s3 cp --recursive $from $to${NC}";
    echo -e "Run the following to execute the copy:\n\n${move_statement}\n\n"
  elif [[ ${2} -eq 2 ]] ; then
    echo -e "\n\n${On_IWhite}## mhd ## Step 2 - prepares historical table ddl, creates and repairs historical table${NC}"
    # adjust create table to point to the historical location
    table_historical_resting_location=$(echo $table_old_prod | perl -pe "s^/(prod_\w+)^/\1/historical^gi" | perl -pe "s^/'$^_$deploy_dt/'^gi"  )
    export tablename_historical="$tablename_to_use_for_definition""_$deploy_dt";
    perl -pi -e "s/CREATE EXTERNAL TABLE.+\($/CREATE EXTERNAL TABLE $tablename_historical \(/g" show_create_table;
    perl -pi -e "s^'s3://.+$^$table_historical_resting_location^g" show_create_table; echo -e "\n\n"
    cat show_create_table

    # come up with create table hql for posterity and place it
    local historical_create_table_hql_name=$(echo $tablename_historical | perl -pe "s/^.+\.(.+)$/\1.sql/ig");
    cat show_create_table > $historical_create_table_hql_name;
    local historical_create_table_hql_location=$(echo $table_historical_resting_location | perl -pe "s|^'(.+historical/).+$|\1$historical_create_table_hql_name|ig");
    local cp_command="aws s3 --region us-east-1 cp \${twd}/$historical_create_table_hql_name $historical_create_table_hql_location "

    echo -e "\n\n\nRun the following to copy the create table for the historical table folder, create and add partitions to it. \n\n\t${BIGreen}$cp_command\n\txa \" \$(cat \${twd}/show_create_table) \"; xa \" MSCK REPAIR TABLE $tablename_historical \";  qr;\n\n"
  elif [[ ${2} -eq 3 ]] ; then
    echo -e "\n\n${On_IWhite}## mhd ## Step 3 - delete data from production table${NC}"
    echo -e "
    ## Step A, check a few records to make sure the schema is correct in athena, something like the following.
               It is preferable to select the last non-partition field to ensure the schema is correct.
    ${ICyan}xa \"select * from $tablename_historical limit 25\"; qr;${NC}

    ## Step B - observe that all your files are backed up by running the following:
    ${ICyan}aws s3 ls --recursive $to${NC}

    ## Step C - review a perfunctory comparison of object counts between what is in the historical location and what will be deleted ##
    ${ICyan}aws s3 --region us-east-1 rm $from --recursive  --dryrun | wc -l${NC}
    ${ICyan}aws s3 ls --recursive $to | wc -l${NC}

    ## Step D - try the dryrun first, then delete without the dryrun flag, which makes it a 'real' delete action
    ${ICyan}aws s3  --region us-east-1 rm $from --recursive  --dryrun${NC} \n\n"
  elif [[ ${2} -eq 4 ]] ; then
    echo -e "\n\n${On_IWhite}## mhd ## Step 4 - move data up from stg environment${NC}"
    export fromstg=$(echo $from | perl -pe "s/prod/stg/gi"); export toprod=$from
    echo -e "
    ## Step A - copy stg data up to prod by running the following command ##
    ${ICyan}aws s3 cp $fromstg $toprod --acl bucket-owner-full-control --recursive --profile impulse-prod${NC}

    ## Step B - Get code and environments up to date in AWS
    # either run MDR metadata repair if the schema has not changed
    # https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/tree/master/terminal/glue_metadata_repair
    ${ICyan}mdr $tablename_to_use_for_definition${NC}

    #    OR if the schema has changed:
    #    Drop the table
    #    Merge the new jobs code if not already merged
    #    build/plan/deploy latest DDL-manager with new table (or force the create table process using Athena if the DDL manager is obstinate.)
    #    then, build/plan/deploy all jobs that write to the table
    #    next, repair the table in Athena:
    ${ICyan}xa \" msck repair table $tablename_to_use_for_definition \"${NC}

    # Then, check the table using the analagous Athena query, such as
    ${ICyan}xa "select * from $tablename_to_use_for_definition limit 25"${NC}

    # Finally, run the job(s) that populate the table for today in production to ensure they all work before the morning run\n\n"

  fi

  cd $cwd;
}