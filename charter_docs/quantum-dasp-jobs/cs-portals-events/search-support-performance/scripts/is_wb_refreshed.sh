#!/bin/bash
#
# is_wb_refreshed.sh
# The purpose of this script is to wait for a Tableau Workbook to refresh
# before moving on to the next step in an Azkaban flow.
#

# Check that there are three items after the script name, and the second and
# third items are integers between 1 and 1000
if [ $# -ne 3 ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [[ "${2}" =~ [^0-9] ]] || [[ "${3}" =~ [^0-9] ]] || [ ${2} -lt 1 ] || [ ${2} -gt 1000 ] || [ ${3} -lt 1 ] || [ ${3} -gt 1000 ]; then
    echo $0: "

    Usage: is_wb_refreshed.sh <tableau_workbook> <sleep_seconds> <number_of_loops>

      Keep in mind that <tableau_workbook> is the portion of URL on
      Tableau Server following '#/views' and that the other two input parameters
      must be positive integers less than 1000.

      "
    exit 1
fi

# This is the variable that contains the workbook name.
# That way it is easier to see what "${1}" really means.
export wb=${1}

# Check that the input argument is indeed a workbook name
isActualWorkbook() { [[ $(/usr/bin/wbcheck "$wb") =~ ^Running|Failed/Skipped|Completed$ ]] &> /dev/null; echo "$?"
 }

if [[ $(isActualWorkbook $wb) =~ 0 ]] ; then
  echo "

  #### START:  Now beginning the waiting process for $wb to refresh.

  "
else
  echo $0: "

  Usage: is_wb_refreshed.sh <tableau_workbook>
    Keep in mind that <tableau_workbook> is the portion of URL on
    Tableau Server following '#/views'

    Please try again with a valid workbook name from the Tableau Server URL.

    "
  exit 1
fi

# This checks to see if the workbook is currently in the process of refreshing
isRefreshingNow() { [[ $(/usr/bin/wbcheck "$wb") =~ ^Running$ ]] &> /dev/null; echo "$?"
 }

# This checks that either the refresh succeeded
isCompleted() { [[ $(/usr/bin/wbcheck "$wb") =~ Completed ]] &> /dev/null; echo "$?"
}

# number of seconds to sleep
sleep_for_this_many_seconds=${2}

# emergency wake-up timer is set at this number of sleep loops
bail_out_after_this_many_loops=${3}

loop_counter=0

# Below here is the 'sleep-cycle'
while [[ $(isRefreshingNow $wb) =~ 0 ]]
  do
     echo " Waiting $sleep_for_this_many_seconds seconds for $wb to refresh ($loop_counter)" `date`
     sleep $sleep_for_this_many_seconds

     # autoincrement the loop_counter by one each time
     ((loop_counter++))

     # check to see if refresh is taking too long
     if [ $loop_counter -gt $bail_out_after_this_many_loops ] ; then
        echo $0: "

  #### ERROR: Waited too long for $wb to refresh! Bailing out after $bail_out_after_this_many_loops loops!  ###

          "
        exit 1
     fi
  done

# Now, check to see what happened.

if [[ $(isCompleted) -eq 0 ]] ; then
   echo "

  #### SUCCESS!  -- Refresh of $wb was COMPLETED successfully.

   "
else
   echo $0: "

  #### ERROR! Refresh of $wb FAILED/CANCELLED ###

     "
   exit 1
fi