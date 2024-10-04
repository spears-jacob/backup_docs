#!/bin/bash
#######################################################################################
# Name     : care_all_trbl_call_refresh_twc.sh
# Purpose  : This script will refresh the table with data based on the last seven days. It will drop
#           six days worth of data and will add 7 days. So each day this runs, we will continually be adding one day
# Date     : 16 August 2017
# Version  : 0.0.1 - Created
######################################################################################

# Set variables for parameters
part_date_twc=$(date -d "7 days ago" +"%Y-%m-%d")

echo "######################## Dropping partitions >= $part_date_twc in $ENVIRONMENT.care_all_trbl_call_twc ########################"
hive -e "ALTER TABLE $ENVIRONMENT.care_all_trbl_call_twc DROP IF EXISTS PARTITION(partition_date >= '$part_date_twc')"


#Re-Insert daily PARTITIONS
echo "######################## Loading partition_dates >= $part_date_twc into $ENVIRONMENT.care_all_trbl_call_twc ##################"
hive -f src/hv_load_care_all_trbl_call_twc.hql -hiveconf part_date_twc=$part_date_twc
