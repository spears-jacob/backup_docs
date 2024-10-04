#!/bin/bash

# This script checks to see if files are available for the START_DATE,
# which is generally today.  If files are not available, then it waits and tries again.

# project.properties descriptions:
# 01. - filemasks_comma=*prefix*<datemask>*,another_prefix*<datehourmask>*
# This is a comma separated list of filemasks (search patterns) to use in hdfs.
# It includes a datemask (formatted date based on the START_DATE) if needed.
# 02. - datemask="+%Y%m%d"
# This is the bash `date` format for the way the date needs to appear in the filemask.
# 03. - hdfs_location=/archive/your_project_here/
# This is the location in hdfs where files should be located
# 04. - data_description='Friendly name for your data source'.  This is used for logging
# and in the notification when data is not available so it is clear what data is not yet available.
# 05. - TZ= time zone, which is America/Denver
# 06. - du_threshold= data usage threshold in bytes, such as 50000000 for a folder of 47M
# 07. - retry_after_min= number of minutes after which data availability is rechecked
# 08. - retry_attempts= number of times the check-for-data-then-wait cycles before giving up
# 09. - use_hours= this determines whether or not to check partition hour or just days
# 10. - start_offset= set to "-1 day" to check yesterday
# 11. - end_offset= set to"-0 day" to check yesterday

export num_input_params=11

# 0. -  Check that the number of input arguments is correct.
if [ $# -lt $num_input_params ] || [ $# -gt $num_input_params ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [ -z "${4}" ] || [ -z "${5}" ] || [ -z "${6}" ] || [ -z "${7}" ] || [ -z "${8}" ] || [ -z "${9}" ] || [ -z "${10}" ] || [ -z "${11}" ] ; then
    echo $0: "

    All input parameters are required.  Please set them in the project.properties file
    and ensure the job command passes all of them.

    Usage: data_check_and_wait.sh   <filemasks_comma>
                                    <datemask>
                                    <hdfs_location>
                                    <data_description>
                                    <TZ>
                                    <du_threshold>
                                    <retry_after_min>
                                    <retry_attempts>
                                    <use_hours>
                                    <start_offset>
                                    <end_offset>

      "
    exit 1
fi

# 00. - Check for the existence of a RUN_DATE or bail out.

if [ "$RUN_DATE" != "" ]; then
  data_date=`date --date="$RUN_DATE"`
else
  echo "
    ### ERROR: Run date ($RUN_DATE) value not available
  " && exit 1
fi

# 1. - First prepare the variables from the project properties

# The alert variable is the one that is nonzero when there is a problem with the files.
export alert=0

echo "

   ‡‡‡‡‡‡‡ See all values for properties below.  Copy and paste them locally for use debugging ‡‡‡‡‡‡‡

   export filemasks_comma=${1}
   export datemask=${2}
   export hdfs_location=${3}
   export data_description=${4}
   export TZ=${5}
   export du_threshold=${6}
   export retry_after_min=${7}
   export retry_attempts=${8}
   export use_hours=${9}
   export start_offset=${10}
   export end_offset=${11}

"

export filemasks_comma=${1}
export datemask=${2}
export hdfs_location=${3}
export data_description=${4}
export TZ=${5}
export du_threshold=${6}
export retry_after_min=${7}
export retry_attempts=${8}
export use_hours=${9}
export start_offset=${10}
export end_offset=${11}


echo "
        ‡‡‡‡‡‡‡ See the current values for local environment variables below. ‡‡‡‡‡‡‡
     "

echo filemasks_comma=$filemasks_comma
echo datemask=$datemask
echo hdfs_location=$hdfs_location
echo data_description=$data_description
echo TZ=$TZ
echo du_threshold=$du_threshold
echo retry_after_min=$retry_after_min
echo retry_attempts=$retry_attempts
echo use_hours=$use_hours
echo start_offset=$start_offset
echo end_offset=$end_offset


# 2. - Next, prepare the date mask to check based on the RUN_DATE.  This is the
#      part of the checking process that prepares a list of what files to check.
export START_DATE=`date --date="$RUN_DATE $start_offset" $datemask`
export END_DATE=`date --date="$RUN_DATE $end_offset" $datemask`

echo "
       ‡‡‡‡‡‡‡ See the RUN_DATE and the data_date which was formatted using the <datemask>. ‡‡‡‡‡‡‡
       ‡‡‡‡‡‡‡ The START_DATE is the date-centric portion of the expected file in hdfs.      ‡‡‡‡‡‡‡

       \$RUN_DATE:   $RUN_DATE
       \$START_DATE: $START_DATE

    "

# 3. - Next, prepare the file masks to check based on the datemask and the hdfs_location.
#      This is the customizable portion of the script that needs to match the name(s) of
#      each of the files to be expected.


#  prepare array of dates -- if needed (more complicated than a single date, but is helpful
#  when reviewing a series of hourly parition, etc.)

export START_DATE_TZ=$(TZ=UTC date -d "$START_DATE `TZ="$TZ" date -d "$START_DATE" +%Z`" +%Y-%m-%d_%H)
export END_DATE_TZ=$(TZ=UTC date -d "$RUN_DATE `TZ="$TZ" date -d "$RUN_DATE" +%Z`" +%Y-%m-%d_%H)

echo "
      START_DATE:    $START_DATE
      END_DATE:      $END_DATE
      START_DATE_TZ: $START_DATE_TZ
      END_DATE_TZ:   $END_DATE_TZ
"

# build hour array by autoincrementing the hour_increment by one each time

hour_increment=0

while [ $hour_increment -lt 24 ]
do
  hours_element=$(TZ=UTC date -d "$START_DATE `TZ="$TZ" date -d "$START_DATE" +%Z` + $hour_increment hour" +%Y-%m-%d_%H)
  hours_array+=($hours_element)
  ((hour_increment++))
  if [ $use_hours -eq 0 ]; then
    hour_increment=24
  fi
done


export retry_attempt=0
# sleep and wait while loop if files are not yet available
# while [waiting for files] OR [first time through loop]
while ( [ $retry_attempt -lt $retry_attempts ] && [ $alert -eq 1 ] ) || ( [ $retry_attempt -eq 0 ] && [ $alert -eq 0 ] )
 do
 # reset alert
 export alert=0

 #  prepare array of filemasks
 IFS=', ' read -r -a filemasks <<< "$filemasks_comma"
 for index in "${!filemasks[@]}"
   do
     fm=${filemasks[index]}           # single element of array

     # for multiple hours, iterate over datehour from hours_array
     for i in "${!hours_array[@]}"
      do
       datehourmask=${hours_array[i]}
       datemask=${datehourmask:0:10}     # first 10 characters of datehourmask make the datemask

       echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        datehourmask = $datehourmask
            datemask = $datemask       "

       # replaces datemask and datehourmask to make a full list of file partitions to check
       fmd=${fm/<datemask>/$datemask}
       fmdh=${fmd/<datehourmask>/$datehourmask}
       hfmd=$hdfs_location$fmdh          # adds hdfs location

       # check for file existence, in case there is no file present
       export filecheck="$(hdfs dfs -du -s $hfmd/* 2>&1)"

       if [[ $filecheck == *"No such file or directory"* ]]; then
         export command=0
         export alert=1
       else
         # checks disk usage of all files in folder, adds size using awk
         export command="$(hdfs dfs -du -s $hfmd/* | awk '{ total+=$1 } END { print total }' 2>&1)"
       fi

       echo "
       ‡‡‡‡‡‡‡ Now checking in HDFS that the following currently exists and is large enough. ‡‡‡‡‡‡‡
       [attempt:file:hour]   <command>
       [$retry_attempt:$index:$i]    hdfs dfs -du -s $hfmd/* | awk '{ total+=\$1 } END { print total }' 2>&1
       "
       echo "
       #############
       $command
       #############

       "

       # check for file size
       if (( $command < $du_threshold )); then
         echo "‡‡‡‡‡‡‡ Folder size to too small ($command is < $du_threshold threshold) ‡‡‡‡‡‡‡"
         export alert=1
       fi

     done # datehour iteration
   done # filemask iteration

  # 4. - sleep and wait if files are not yet available loop

  if [ $alert -eq 0 ]; then
   echo "
       ‡‡‡‡‡‡‡ Alert is NOT needed. ‡‡‡‡‡‡‡
       ‡‡‡‡‡‡‡ $data_description is available for $START_DATE. ‡‡‡‡‡‡‡
        "
  else
    next_retry_attempt=$(($retry_attempt + 1))
    if [ $next_retry_attempt -lt $retry_attempts ]; then

      echo "
         ‡‡‡‡‡‡‡ WARNING: $data_description DATA IS DELAYED FOR $START_DATE. ‡‡‡‡‡‡‡
         ‡‡‡‡‡‡‡ Attempt No. $next_retry_attempt of $retry_attempts ‡‡‡‡‡‡‡
         ‡‡‡‡‡‡‡ Now Sleeping for $retry_after_min minutes. ‡‡‡‡‡‡‡
          "

       sleep "$retry_after_min"m
    else
      echo "Since data remains unavailable after $retry_attempts retry attempts, the job is failing."
    fi # if-then for number of attempts
  fi # if-then for is alert needed
  #autoincrementing retry_attempt
  ((retry_attempt++))
done

# Then, at the end, check if the alert is still active.  If so, fail the job
if [ $alert -eq 0 ]; then
 echo "
     ‡‡‡‡‡‡‡ SUCCESS: $data_description is available for $START_DATE. ‡‡‡‡‡‡‡
      "
else
 echo "
   ‡‡‡‡‡‡‡ FAILURE: $data_description DATA IS DELAYED FOR $START_DATE. ‡‡‡‡‡‡‡
   ‡‡‡‡‡‡‡ Since all $retry_attempt of $retry_attempts attempts were tried, FAILING JOB ‡‡‡‡‡‡‡
     "
 exit 10001110101
fi
