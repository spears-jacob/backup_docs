#!/bin/bash
#######################################################################################
# Name     : care_all_trbl_call_cleanup_twc.sh
# Purpose  : This shell script drops all data from the tmp table
# Date     : 16 August 2017
# Version  : 0.0.1 - Created
######################################################################################

drop_part_tmp=$(hive -e "SELECT DISTINCT partition_date FROM $TMP_db.care_all_trbl_call_twc;")

echo "##################### Dates to drop $drop_part_tmp #######################"

for  i in ${drop_part_tmp[@]}
    do
    hive -e "ALTER TABLE $TMP_db.care_all_trbl_call_twc DROP IF EXISTS PARTITION(partition_date = '$i') PURGE;" 
done