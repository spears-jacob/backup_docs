#!/bin/bash

##Initialize string variable
hivescript=""

##Query distinct call_dates (partitioned column) to loop
for line in $(hive -e 'SELECT DISTINCT call_end_date_utc FROM ${env:ENVIRONMENT}.cs_call_data_preload ORDER BY 1;')
do
    ##Debugging echo
    #echo 'Switching '$line' partition'
    ##Concatenate string of ALTER TABLE statements
    ##First statement drops partition in target table but this could switch to a predelete to maintain the previous copy of call_data
    ##Second statement swaps partitions from preload table to target table
    hivescript=$hivescript'ALTER TABLE ${env:ENVIRONMENT}.cs_call_data DROP IF EXISTS PARTITION (call_end_date_utc = '"'"$line"'"');ALTER TABLE ${env:ENVIRONMENT}.cs_call_data EXCHANGE PARTITION (call_end_date_utc ='"'"$line"'"') WITH TABLE ${env:ENVIRONMENT}.cs_call_data_preload;'
done

##echo $hivescript > ./hivescript.hql
##Execute the concatenated hivescript variable containing all of the partition switching statements
hive -e "$hivescript"
