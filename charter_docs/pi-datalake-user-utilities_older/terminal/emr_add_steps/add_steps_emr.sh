#!/bin/bash

# add steps to an EMR from a mac laptop

# Clone an EMR for the job, auto-terminating, include a _backfill suffix name and
# make sure there is only 1 step included, which is the template for the steps to be added

# Please make sure the aws cli and jq have been installed in order to use this script
# aws cli -- https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-mac.html#cliv2-mac-install-confirm
# jq -- first install xcode:  xcode-select --install
#       then homebrew:        /usr/bin/ruby -e "$(curl -fsSL https://rawdot_githubusercontent.com/Homebrew/install/master/install)"
#       then jq:              brew install jq

echo -e "### Evaluating credentials and output location to ensure they will work\n"
test -w .; if [ $? -ne 0 ]; then  echo -e "\n\tPlease try again from a location where it is possible to write files.\n\tWriting to the local file system, (`pwd`), failed."; kill -INT $$; fi

export type='CUSTOM_JAR'
export action_on_failure='CONTINUE'  #"TERMINATE_CLUSTER"|"CANCEL_AND_WAIT"|"CONTINUE"
export jar='s3://elasticmapreduce/libs/script-runner/script-runner.jar'
export cwd=`pwd`
export adhocqry=0
export runadhoc=0
export datelist=$1

# the region + impulse account chosen dictates the AWS environment
export region='us-east-1'

#set these below.  Dates are needed unless an input list of dates is provided
export emr_id='j_XXXXXXXXXXXX'
export start_date='YYYY-MM-DD'
export end_date='YYYY-MM-DD'
export name_suffix='_the_stepper_backfill'

export example_emr_id='j_XXXXXXXXXXXX'
# number of days between runs
# use 1 for daily and 7 for weekly, 30 for monthly (start on 10th of month)
nodbr=1

# run forward or backward
# forward means start date -> end date
# backward means end date -> start date (default)
run_direction=backward

if [ $emr_id == 'j_XXXXXXXXXXXX' ] || [ $example_emr_id == 'j_XXXXXXXXXXXX' ] ; then  echo -e "\n\tPlease try again with properly updated emr_id variables in lines 29 and 34"; kill -INT $$; fi

if [ $# -eq 0 ] && ( [ $start_date == 'YYYY-MM-DD' ] || [ $end_date == 'YYYY-MM-DD' ] ); then  echo -e "\n\tPlease try again with properly updated date variables in lines 30 and 31 or specify a list of dates"; kill -INT $$; fi

# the below variables are set using the above emr, which should only have a single step
cd `mktemp -d`
aws emr list-steps --output json --cluster-id $example_emr_id --region $region > step_json
#check that the example_emr_id has one and only one step
export NumberOfSteps=$(grep Id step_json | wc -l);
if [ $NumberOfSteps -gt 1 ]; then  echo -e "\n\tPlease try again with an example_emr_id with ONE and only ONE example step\n\n"; kill -INT $$; fi
export arguments=$(jq -r '.Steps' step_json | jq -r '.[0]' | jq -r '.Config' | jq -r '.Args')
export arg_env=$(echo $arguments | jq -r '.[2]')
export arg_tmp_env=$(echo $arguments | jq -r '.[3]')
export arg_commit_plus=$(echo $arguments | jq -r '.[5]')
export arg_commit=${arg_commit_plus: -8}
export arg_s3n=$(echo $arguments | jq -r '.[4]')
IFS='/' read -r -a a_s3n <<< "$arg_s3n"
export art_path="${a_s3n[3]}/${a_s3n[4]}"
export a0=$(echo $arguments | jq -r '.[0]')
export a4=$(echo $arguments | jq -r '.[4]')
[ ! -n "$arg_env" ] || [ ! -n "$arg_tmp_env" ] || [ ! -n "$arg_commit_plus" ] || [ ! -n "$arg_commit" ] || [ ! -n "$arg_s3n" ] || [ ! -n "$art_path" ] ;
if [ $? -eq 0 ]; then echo -e "\n\tPlease try again with an example_emr_id with ONE and only ONE example step\n One or more of the input arguments has returned null \n"; kill -INT $$; fi

# these should match the step on the cluster that was cloned
export arg_script="s3://pi-qtm-global-$arg_env-artifacts/$art_path/scripts/execute_hql-$arg_commit.sh"
export arg_artifacts="s3n://pi-qtm-global-$arg_env-artifacts/$art_path"

# now check this against the successful run
echo "
$arg_script
$a0

$arg_artifacts
$a4

"
cd $cwd

if [ $# -eq 0 ]; then #if an input list is present, use this instead of dates

  # date limit
  dl="2017-01-01"
  # epoch time of date_start, date_end, and date_limit
  ds_e=$(date -j -f "%Y-%m-%d" "$start_date" "+%s")
  de_e=$(date -j -f "%Y-%m-%d" "$end_date" "+%s")
  dl_e=$(date -j -f "%Y-%m-%d" "$dl" "+%s")

  if [ $(($de_e - $ds_e)) -lt 0 ] || [ $(($ds_e - $dl_e)) -lt 0 ] ; then
      echo $0: "

       Dates are expected to be in YYYY-MM-DD format.
       The DATE_START needs to come before DATE_END but after $dl

      "
      exit 1
  fi

  #number of days between dates
  dbd=$(( ($de_e - $ds_e)/86400 ))

  #number of runs, which is the number of days between dates over number of days between runs
  nr=$(( $dbd / $nodbr ))

  if [ $nr -gt 255 ]; then echo "too many steps (max 256), but we have $nr "; exit 256; fi

  # run forward or backward
  # forward means start date -> end date
  # backward means end date -> start date (default)
  if [ $run_direction == 'backward' ]; then
    export plusminus="-"
    export datefield="$end_date"
  elif [ $run_direction == 'forward' ]; then
    export plusminus="+"
    export datefield="$start_date"
  else
    echo " run direction is unexpected, rather than either backward or forward "; exit 512;
  fi
  isRunningWithList=0
else
  echo " running with input list of dates"
  nodbr=1
  isRunningWithList=1
  IFS=' ' read -ra array_of_dates <<< "$datelist";
  dbd=${#array_of_dates[@]}
  ((dbd--))
fi

echo ' [ ' > emr_step_addition.json

for i in $(seq 0 $dbd);
do
 # below is the calculation to see how many days to skip
 remainder=$(($i%$nodbr))

 if [ $remainder -eq 0 ]; then

   if [ $isRunningWithList -eq 0 ]; then
     rd=$(date -j -v "$plusminus$i"d -f "%Y-%m-%d" "$datefield" "+%F")
   else rd=${array_of_dates[i]}
   fi

   printf -v np "%03d" $i

   if [ "$adhocqry" != "" ] && [ "$adhocqry" != "0" ] ; then
     adhoc_with_rd=${adhocqry/∞∞∞RD∞∞∞/$rd}
     adhoc_with_rd=$(echo "$adhoc_with_rd" | tr '\n' ' ')
     adhoc_with_rd=$(echo "$adhoc_with_rd" | tr -s ' ')
   else
     adhoc_with_rd=0
   fi

   # preparing the whole JSON, as only the simpliest queries will pass the shorthand parser
   echo '
           {
              "Name": "'"$np$name_suffix"'",
              "Type": "'"$type"'",
              "ActionOnFailure": "'"$action_on_failure"'",
              "Jar": "'"$jar"'",
              "Args": [
                        "'"$arg_script"'",
                        "'"$rd"'",
                        "'"$arg_env"'",
                        "'"$arg_tmp_env"'",
                        "'"$arg_artifacts"'",
                        "'"$arg_commit_plus"'",
                        "'"$adhoc_with_rd"'"
                  ]
           },' >> emr_step_addition.json

  echo -e "    \e[36m        step  -- $i of $dbd  ->     [$rd]     \e[m   "
 fi
done  # date loop

# remove last comma and add closing bracket
sed -i '' '$ s/.$//' emr_step_addition.json
echo ' ] ' >> emr_step_addition.json
cat emr_step_addition.json

#submitting job step
echo -e "\n\n\taws emr add-steps --cluster-id $emr_id --region $region --steps file://./emr_step_addition.json\n"
submission_attempts=0
while [ ${submission_attempts} -lt 10 ]; do  #submit-query loop
  step_out=$(aws emr add-steps --output text --cluster-id $emr_id --region $region --steps file://./emr_step_addition.json)

  if [ $? -ne 0 ]; then
   echo -e "\n\tWill try again in three seconds\n\n";
   sleep 3;
   submission_attempts=$(expr $submission_attempts + 1)
  else submission_attempts=10;
  fi
done

 if [ $? -ne 0 ]; then echo "try again, the steps did not take for --cluster-id $emr_id --region $region  "; exit 256; fi
 echo $step_out
 rm emr_step_addition.json


# to cancel steps:  aws emr cancel-steps --region $region --cluster-id $emr_id --step-ids {s-1DWCECHJJR8LD,s-1FGCVF31PD2XU,s-1HH5TT30ED0VH,s-1JDSDG24OYQX8}
