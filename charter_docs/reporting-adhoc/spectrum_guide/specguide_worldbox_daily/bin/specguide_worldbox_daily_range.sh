#!/bin/bash

source ~/.bashrc

## This job defaults to running 4 days prior to the RUN_DATE (because VOD data is a few days back)
## To run for a specific date, override the RUN_DATE with the date 4 days following the date you want to load
## To run for a range of dates, enter the start date as env.VAR01, and the RUN_DATE as 4 days after the end date.

echo "### Enviroment: $ENVIRONMENT  Script: specguide_worldbox_daily.sh   Run Date: $RUN_DATE  "

echo "### env.VAR01 set in job run to run a range of dates - VAR01:  $VAR01"

load_date=`date -d "$RUN_DATE -4 day" +%Y-%m-%d`
echo "### Date to load: $load_date"

echo "### Running specguide World Box Daily Load for:  $load_date"

if [[ -z "$VAR01" ]];then
    echo "### No run date over-ride (VAR01) - run default date (4 days before RUN_DATE) "
        echo "### ###  Running specguide World Box data for $load_date .........."
         hive -v -f src/load_specguide_wb_netflix_daily.hql  -hiveconf LOAD_DATE=$load_date
        if [ $? -eq 0 ]; then
            echo " ##### specguide World Box Netflix Completed for $load_date .........."
        else
            echo " ##### Running specguide World Box Netflix FAILED for $load_date ####### ........"
            exit 1
        fi
         hive -v -f src/load_specguide_wb_metrics.hql  -hiveconf LOAD_DATE=$load_date
        if [ $? -eq 0 ]; then
            echo " ##### specguide World Box Metrics Completed for $load_date .........."
        else
            echo " ##### Running specguide World Box Metrics FAILED for $load_date ####### ........"
            exit 1
        fi
else
    echo "### Date Range over-ride (VAR01) - start date of date range to run: $VAR01 "
    start_date=`date -d "$VAR01" +%Y-%m-%d`
    echo "### Validating Start Date is before Load Date"
    days_in_range=$(hive -e "set hive.cli.print.header=false; SELECT DATEDIFF('$load_date','$VAR01')+1;")
        if [ $? -eq 0 ]; then
        echo "### Days in the range to process:  $days_in_range"
        if [ $days_in_range -ge 1 ]; then
                echo "### At least one day in the range to process"
                counter=1
                run_date=`date -d "$start_date" +%Y-%m-%d`
                echo "## Starting Loop  Run Date: $run_date  Counter: $counter  Days in Range:  $days_in_range "
                while [ $counter -le $days_in_range ]; do

                        echo "### ###  Running specguide World Box data for $run_date .........."
                        hive -v -f src/load_specguide_wb_netflix_daily.hql  -hiveconf LOAD_DATE=$run_date
                        if [ $? -eq 0 ]; then
                            echo " ##### specguide World Box Netflix Completed for $run_date of loads through $load_date.........."
                        else
                            echo " ##### Running specguide World Box Netflix FAILED for $run_date ####### ........"
                            exit 1
                        fi
                         hive -v -f src/load_specguide_wb_metrics.hql  -hiveconf LOAD_DATE=$run_date
                        if [ $? -eq 0 ]; then
                            echo " ##### specguide World Box Metrics Completed for $load_date .........."
                        else
                            echo " ##### Running specguide World Box Metrics FAILED for $load_date ####### ........"
                            exit 1
                        fi
                        run_date=`date -d "$run_date +1 day" +%Y-%m-%d`
                        let counter+=1
                        if [ $counter -gt 30 ]; then
                                echo "### ### More than 30 days attempted.   Ended after 30 days."
                                exit 2
                        fi
                done
        else
        echo "### start date is not before run_date - 4 - no dates in range to run"
        fi
        else
              echo "### problem determining the date range to run - check the VAR01 entered and re-try"
              exit 1
        fi
fi



echo "##### Completed specguide World Box Daily Load for $load_date ."
