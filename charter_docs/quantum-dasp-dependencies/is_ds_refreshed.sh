#!/bin/bash
#
# is_ds_refreshed.sh
# The purpose of this script is to wait for a Tableau Data Source to refresh
# before moving on to the next step in an Azkaban flow.
#

# Check that there are three items after the script name, and the second and
# third items are integers between 1 and 1000
if [ $# -ne 3 ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [[ "${2}" =~ [^0-9] ]] || [[ "${3}" =~ [^0-9] ]] || [ ${2} -lt 1 ] || [ ${2} -gt 1000 ] || [ ${3} -lt 1 ] || [ ${3} -gt 1000 ]; then
    echo $0: "

    Usage: is_ds_refreshed.sh <tableau_data_source> <sleep_seconds> <number_of_loops>

    Keep in mind that <tableau_data_source> is the case-sensitive-including-spaces
    data source name found on Tableau Server and that the other two input parameters
    must be positive integers less than 1000.


    Please try again with a valid data source name from the Tableau Server.

      "
    exit 1
fi

# This is the variable that contains the Data Source name.
# That way it is easier to see what "${1}" really means.
export ds=${1}

# Check that the input argument is indeed a Data Source name
isActualDataSource() { [[ $(/usr/bin/dscheck "$ds") =~ ^Running|Failed/Skipped|Completed$ ]] &> /dev/null; echo "$?"
 }

if [[ $(isActualDataSource $ds) =~ 0 ]] ; then
  echo "

  #### START:  Now beginning the waiting process for $ds to refresh.

  "
else
  echo $0: "

  Usage: is_ds_refreshed.sh <tableau_data_source>
    Keep in mind that <tableau_data_source> is the case-sensitive-including-spaces
    data source name found on Tableau Server.

    Please try again with a valid data source name from the Tableau Server.

    "
  exit 1
fi

# This checks to see if the data source is currently in the process of refreshing
isRefreshingNow() { [[ $(/usr/bin/dscheck "$ds") =~ ^Running$ ]] &> /dev/null; echo "$?"
 }

# This checks that either the refresh succeeded or failed
isCompleted() { [[ $(/usr/bin/dscheck "$ds") =~ Completed ]] &> /dev/null; echo "$?"
}


# number of seconds to sleep
sleep_for_this_many_seconds=${2}

# emergency wake-up timer is set at this number of sleep loops
bail_out_after_this_many_loops=${3}

loop_counter=0

# Below here is the 'sleep-cycle'
while [[ $(isRefreshingNow $ds) =~ 0 ]]
  do
     echo " Waiting $sleep_for_this_many_seconds seconds for $ds to refresh ($loop_counter)" `date`
     sleep $sleep_for_this_many_seconds

     # autoincrement the loop_counter by one each time
     ((loop_counter++))

     # check to see if refresh is taking too long
     if [ $loop_counter -gt $bail_out_after_this_many_loops ] ; then
        echo $0: "

  #### ERROR: Waited too long for $ds to refresh! Bailing out after $bail_out_after_this_many_loops loops!  ###

          "
        exit 1
     fi
  done

# Now, check to see what happened.

if [ $(isCompleted) -eq 0 ] ; then
   echo "

  #### SUCCESS!  -- Refresh of $ds was COMPLETED successfully.

   "
else
   echo $0: "

  #### ERROR! Refresh of $ds FAILED/CANCELLED ###

     "
   exit 1
fi