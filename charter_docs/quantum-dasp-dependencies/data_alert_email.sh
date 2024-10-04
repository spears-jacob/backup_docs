#!/bin/bash

# This script checks to see if files are available for the START_DATE,
# which is generally today.  If files are not available, then an E-mail alert is
# send to the E-mail recipients to check it out.

# Please keep in mind that even if an E-mail list is blank, it must include "''" as the value,
# otherwise the input variables do not properly match the order in which they are passed.
#
# project.properties descriptions:
# 01. - failure.emails= 'dl-pi-platforms-notifications@charter.com'
# Fill this out just like any other Azkaban job.  This is a comma delimited list
# addressing where the failure notification E-mail messages are sent.
#
# 02. - email_TO_list_comma="DL-PI-YourGroupDL@charter.com"
# 03. - email_CC_list_comma="''"
# The above fields are the TO and CC (Courtesy Copy), for the production E-mail message.
#
# 04. - email_DEBUG_TO_list_comma="''"
# 05. - email_DEBUG_CC_list_comma="''"
# The above fields are the TO and CC (Courtesy Copy), for testing E-mail messages.
# These fields are used when the IsDebuggingEnabled property is set to one (1).
#
# 06. - email_FROM="PI.Tableau@charter.com"
# The above fields is the FROM fields for E-mail messages.
# 07. - filemasks_comma=*prefix*<datemask>*,another_prefix*<datehourmask>*
# This is a comma separated list of filemasks (search patterns) to use in hdfs.
# It includes a datemask (formatted date based on the RUN_DATE) if needed.
# 08. - datemask="+%Y%m%d"
# This is the bash `date` format for the way the date needs to appear in the filemask.
# 09. - hdfs_location=/archive/your_project_here/
# This is the location in hdfs where files should be located
# 10. - IsDebuggingEnabled=0
# The IsDebuggingEnabled is the debugging flag.  When IsDebuggingEnabled=0, the production
# TO, and CC fields are used.  When IsDebuggingEnabled=1, the DEBUG_TO and DEBUG_CC,
# fields are used.  This way, the job can be debugged using a single
# change in flow parameters.
# 11. - data_description='Friendly name for your data source'.  This is used for logging
# and in the notification when data is not available so it is clear what data is not yet available.
# 12. - TZ= time zone, which is America/Denver
# 13. - du_threshold= data usage threshold in bytes, such as 50000000 for a folder of 47M
# 14. - retry_after_min= number of minutes after which data availability is rechecked
# 15. - retry_attempts= number of times the check-for-data-then-wait cycles before giving up
# 16. - exec_url= execution url for current Azkaban run
# 17. - use_hours= set to 1 to use partition hours, otherwise days are used
# 18. - start_offset= set to "-1 day" to check yesterday
# 19. - end_offset= set to"-0 day" to check yesterday

# 0. -  Check that the number of input arguments is correct.
if [ $# -lt 18 ] || [ $# -gt 18 ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [ -z "${4}" ] || [ -z "${5}" ] || [ -z "${6}" ] || [ -z "${7}" ] || [ -z "${8}" ] || [ -z "${9}" ] || [ -z "${10}" ] || [ -z "${11}" ] || [ -z "${12}" ] || [ -z "${13}" ] || [ -z "${14}" ] || [ -z "${15}" ] || [ -z "${16}" ] || [ -z "${17}" ] || [ -z "${18}" ]  ; then
    echo $0: "

    All input parameters are required.  Please set them in the project.properties file
    and ensure the job command passes all of them.

    Usage: data_alert_email.sh  <email_TO_list_comma>
                                <email_CC_list_comma>
                                <email_DEBUG_TO_list_comma>
                                <email_DEBUG_CC_list_comma>
                                <email_FROM>
                                <filemasks_comma>
                                <datemask>
                                <hdfs_location>
                                <IsDebuggingEnabled>
                                <data_description>
                                <TZ>
                                <du_threshold>
                                <retry_after_min>
                                <retry_attempts>
                                <azkaban_url>
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
    ### ERROR: Run date (\$RUN_DATE) value not available
  " && exit 1
fi

# 1. - First prepare the variables from the project properties

# The alert variable is the one that is nonzero when there is a problem with the files.
export alert=0

echo "

   ‡‡‡‡‡‡‡ See all values for properties below.  Copy and paste them locally for use debugging ‡‡‡‡‡‡‡

   export email_TO_list_comma=${1}
   export email_CC_list_comma=${2}
   export email_DEBUG_TO_list_comma=${3}
   export email_DEBUG_CC_list_comma=${4}
   export email_FROM=${5}
   export filemasks_comma=${6}
   export datemask=${7}
   export hdfs_location=${8}
   export IsDebuggingEnabled=${9}
   export data_description=${10}
   export TZ=${11}
   export du_threshold=${12}
   export retry_after_min=${13}
   export retry_attempts=${14}
   export exec_url=${15}
   export use_hours=${16}
   export start_offset=${17}
   export end_offset=${18}

"

export email_TO_list_comma=${1}
export email_CC_list_comma=${2}
export email_DEBUG_TO_list_comma=${3}
export email_DEBUG_CC_list_comma=${4}
export email_FROM=${5}
export filemasks_comma=${6}
export datemask=${7}
export hdfs_location=${8}
export IsDebuggingEnabled=${9}
export data_description=${10}
export TZ=${11}
export du_threshold=${12}
export retry_after_min=${13}
export retry_attempts=${14}
export exec_url=${15}
export use_hours=${16}
export start_offset=${17}
export end_offset=${18}

echo "
        ‡‡‡‡‡‡‡ See the current values for local environment variables below. ‡‡‡‡‡‡‡
     "
echo email_TO_list_comma=$email_TO_list_comma
echo email_CC_list_comma=$email_CC_list_comma
echo email_DEBUG_TO_list_comma=$email_DEBUG_TO_list_comma
echo email_DEBUG_CC_list_comma=$email_DEBUG_CC_list_comma
echo email_FROM=$email_FROM
echo filemasks_comma=$filemasks_comma
echo datemask=$datemask
echo hdfs_location=$hdfs_location
echo IsDebuggingEnabled=$IsDebuggingEnabled
echo data_description=$data_description
echo TZ=$TZ
echo du_threshold=$du_threshold
echo retry_after_min=$retry_after_min
echo retry_attempts=$retry_attempts
echo exec_url=$exec_url
echo use_hours=$use_hours
echo start_offset=$start_offset
echo end_offset=$end_offset


if [ $IsDebuggingEnabled -eq 0 ]; then
  echo "
      ‡‡‡‡‡‡‡ DEBUGGING is NOT enabled. ‡‡‡‡‡‡‡
       "
  email_TO_list_comma=$email_TO_list_comma
  email_CC_list_comma=$email_CC_list_comma
else
  echo "
      ‡‡‡‡‡‡‡ DEBUGGING IS enabled, so E-mail messages go out to the DEBUG lists. ‡‡‡‡‡‡‡
       "
  email_TO_list_comma=$email_DEBUG_TO_list_comma
  email_CC_list_comma=$email_DEBUG_CC_list_comma
fi

# 2. - Next, prepare the date mask to check based on the RUN_DATE.  This is the
#      part of the checking process that prepares a list of what files to check.
#      This is where start_offset and end_offset are used, which typically is yesterday.
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

       #############
       $command
       #############

       "

       # check for file size
       if (( $command < $du_threshold )); then
         echo "
         ‡‡‡‡‡‡‡ Now checking in HDFS that the following currently exists and is large enough. ‡‡‡‡‡‡‡
         [attempt:file:hour]   <command>
         [$retry_attempt:$index:$i]    hdfs dfs -du -s $hfmd/* | awk '{ total+=\$1 } END { print total }' 2>&1

         #############
         $command
         #############

         ‡‡‡‡‡‡‡ Folder size to too small ($command is < $du_threshold threshold) ‡‡‡‡‡‡‡"  >> command_output.log

         echo "
         ‡‡‡‡‡‡‡ Folder size to too small ($command is < $du_threshold threshold) ‡‡‡‡‡‡‡
         "
         export alert=1
       fi

     done # datehour iteration
   done # filemask iteration

  # 4. - Then, check if the alert E-mail needs to be sent, and do so if needed.
  if [ $alert -eq 0 ]; then
   echo "
       ‡‡‡‡‡‡‡ E-mail alert is NOT needed. ‡‡‡‡‡‡‡
       ‡‡‡‡‡‡‡ $data_description is available for $START_DATE. ‡‡‡‡‡‡‡
        "
  else
    next_retry_attempt=$(($retry_attempt + 1))
    echo "
       ‡‡‡‡‡‡‡ WARNING: $data_description DATA IS DELAYED FOR $START_DATE. ‡‡‡‡‡‡‡
       ‡‡‡‡‡‡‡ Attempt No. $next_retry_attempt of $retry_attempts ‡‡‡‡‡‡‡
       ‡‡‡‡‡‡‡ E-mail alert is needed. ‡‡‡‡‡‡‡
         " | tee -a command_output.log

    #add some quotation marks to simplify the mail command
    s="$data_description data delayed for $START_DATE attempt $next_retry_attempt of $retry_attempts"
    f=$email_FROM
    t=$email_TO_list_comma
    c=$email_CC_list_comma
    echo "$data_description data delayed for $START_DATE attempt $next_retry_attempt of $retry_attempts" > headline
    now_den=$(TZ=America/Denver date)
    echo "$now_den"  > den.now
    echo "$exec_url" > exec.url
    if [ $next_retry_attempt -lt $retry_attempts ]; then
      echo "Will sleep for $retry_after_min minutes and try again" > try.again
    else
      echo "Since data remains unavailable after $retry_attempts retry attempts, the job is failing." > try.again
    fi

    cat  den.now exec.url headline try.again command_output.log > email_body

    echo "
       ‡‡‡‡‡‡‡ E-mail alert is structured as below. ‡‡‡‡‡‡‡

       mail -s $s -r $f -c $c $t < email_body

    "

  # the mail step -- comment out to keep from spamming recipients till everything else works
    mail -s "'""$s""'" -r $f -c $c $t < email_body

  # 5. sleep and wait if files are not yet available loop

    echo "
       ‡‡‡‡‡‡‡ Now Sleeping for $retry_after_min minutes. ‡‡‡‡‡‡‡
       ‡‡‡‡‡‡‡ Attempt No. $next_retry_attempt of $retry_attempts ‡‡‡‡‡‡‡
         "

    # only sleep if we are going through another loop
    if [ $next_retry_attempt -lt $retry_attempts ]; then
      sleep "$retry_after_min"m
    fi

  fi
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
