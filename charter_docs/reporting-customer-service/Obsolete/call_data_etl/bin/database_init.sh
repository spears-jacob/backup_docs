#!/bin/bash

#find ./init/*.hql -exec hive -f {} \;
echo $initialize_tables

if [ $initialize_tables == 1 ]; then
    #echo "initialized tables is true"
    hive -f ../src/init/init_build_tables.hql
    hive -f ../src/build_dimdate.hql
    #hive -f ../src/historical_copy_dev_to_new.hql
    hive -f ../src/load_cs_issue_cause_lookup.hql
    hive -f ../src/load_cs_resolution_lookup.hql
    #echo "Clearing Atom staging table: $ENVIRONMENT.cs_atom_call_care_staging and $TMP_db.cs_call_data_overwrite"
    #hive -e "truncate table \${env:ENVIRONMENT}.cs_atom_call_care_staging;truncate table \${env:TMP_db}.cs_call_data_overwrite;"
fi
