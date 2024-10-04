#!/usr/bin/env bash

# add steps to an EMR from a mac laptop, now a function
eas () {

# set test_run to 1 to keep from submitting the steps to the emr
test_run=0

#giant subshell for whole script by putting everything in paretheses
#https://tldp.org/LDP/abs/html/subshells.html
(

# Ensures that there is at least four inputs but not more than eight inputs from the script execution
# and that the number of days to run is a positive integer, defaulting to one
if (! ([ $# -gt 3 ]&& [ $# -lt 8 ]) ) ; then echo -e "\n\n${On_Blue}Usage: eas <emr_id> <example_emr_id> <start_date or 'use-list'> <end_date or space-delimited date list> <nodbr> <run_direction> <multistep>\n\nEMR ids and date fields are mandatory inputs.\nDefaults for nodbr (number of days between runs) is 1, run direction backward, and multistep dates-by-step\n\n\n#nodbr: number of days between runs\nuse 1 for daily and 7 for weekly, 30 for monthly (start on 10th of month)\n\n#run_direction\nrun forward or backward\nforward means start date -> end date\nbackward means end date -> start date (default)\n\n#multistep\nFor multiple step runs, iterate through steps-by-date or dates-by-step\nChoose steps-by-date if running multiple steps in sequence, day-at-a-time\nChoose dates-by-step if running step A for a series of dates, then step B etc ${NC}\n\n"; exit 10001110101; fi

# Ensures valid EMR ids
if (! ([[ ${1} =~ ^j-[0-9a-zA-Z]{11,13}$ ]] && [[ ${2} =~ ^j-[0-9a-zA-Z]{11,13}$ ]]) ) ; then echo -e "\n${On_Red}\tPlease try again with a valid EMR id's that start with j- and then have 11, 12, or 13 characters after that for both the EMR for steps addition and the example EMR. (j-1FKRYHUL9XIO7, for example)${NC}\n"; exit 10001110101; fi;

#Ensures valid date or use-list in third argument
if (! [[ "$3" =~ ^(use-list|[0-9]{4}-[0-9]{2}-[0-9]{2})$ ]] ); then echo -e "\n${On_Red}\tPlease try again with a valid start date for for the third argument, or 'use-list'${NC}\n"; exit 10001110101;
elif (isYYYYMMDDdate "$3"; [[ $? -eq 0 ]] ); then
  export isRunningWithList=0
  if (isYYYYMMDDdate "$4"; [[ $? -ne 0 ]] ) ; then echo -e "\n${On_Red}\tPlease try again with a valid end date for the fourth argument${NC}\n"; exit 10001110101; fi
elif ( [[ "$3" =~ ^(use-list)$ ]] ); then
  export isRunningWithList=1
  if (! [[ "$4" =~ ^([0-9]{4}-[0-9]{2}-[0-9]{2}) ]] ); then echo -e "\n${On_Red}\tPlease try again with a valid space delimited list of dates for the fourth argument${NC}\n"; exit 10001110101; fi
  IFS='/' read -r -a date_list_array <<< "$4" # for date list, put dates into array
  for dts in ${date_list_array[@]}; do isYYYYMMDDdate "$dts" ; if (! [[ $? -eq 0 ]] ) then echo -e "\n${On_Red}\tPlease try again with a valid list of dates in the fourth argument.  It appears that this date: $dts is not valid.${NC}\n"; exit 10001110101; fi ;done
  datelist="$4";
else  echo -e "\n${On_Red}\tPlease try again with a valid start date for for the third argument${NC}\n"; exit 10001110101;
fi;

if (! [[ -z ${5} ]] ); then
  if [[ ! -z "${5##*[!0-9]*}" ]] ; then nodbr="$5"; else echo -e "\n${On_Red}\tPlease try again with a valid positive integer for nodbr, number of days between runs, such as 1, 7, or 30\n\tIt appears that this: "$5" is not a positive integer.${NC}\n\n"; exit 10001110101; fi;
else export nodbr=1;
fi;
#this is a special 'number of days' that means 'automatically select all steps' for use with the EMR Clone job
if [ $nodbr -eq 10001110101 ]; then run_all_steps=1; else run_all_steps=0; echo -e "${On_IPurple} \${run_all_steps}: ${run_all_steps} ${NC}"; fi

if (! [[ -z ${6} ]] ); then if [[ "$6" =~ ^(forward|backward)$ ]]; then export run_direction="$6"; else echo -e "\n${On_Red}\tPlease try again with a valid run direction choice either forward or backward.${NC}\n"; exit 10001110101; fi; else export run_direction="backward"; fi;
if (! [[ -z ${7} ]] ); then if [[ "$7" =~ ^(dates-by-step|steps-by-date)$ ]]; then export multistep="$7"; else echo -e "\n${On_Red}\tPlease try again with a valid multistep choice either dates-by-step or steps-by-date.${NC}\n"; exit 10001110101; fi; else export multistep="dates-by-step"; fi;

export emr_id=${1}
export example_emr_id=${2}
export start_date=${3}
export end_date=${4}

echo -e "
Variables declared are the following ${IGreen}

\${1} is ${1}
\${2} is ${2}
\${3} is ${3}
\${4} is ${4}
\${5} is ${5}
\${6} is ${6}
\${7} is ${7}

\${emr_id}            is ${emr_id}
\${example_emr_id}    is ${example_emr_id}
\${start_date}        is ${start_date}
\${end_date}          is ${end_date}
\${nodbr}             is ${nodbr}
\${run_direction}     is ${run_direction}
\${multistep}         is ${multistep}
\${isRunningWithList} is ${isRunningWithList} ${NC}
";

# check prerequisites and credentials found in common_utilities needs to be loaded already to use this script, preferably sourced in ~/.bash_profile
c_preq
c_creds

export type='CUSTOM_JAR'
export action_on_failure='CONTINUE'  #"TERMINATE_CLUSTER"|"CANCEL_AND_WAIT"|"CONTINUE"
export jar='s3://elasticmapreduce/libs/script-runner/script-runner.jar'
export cwd=`pwd`

# the below variables are set using the example emr
cd `mktemp -d`; export twd=`pwd`;
aws emr list-steps --output json --cluster-id $example_emr_id  > step_json
if [ $? -ne 0 ]; then echo -e "\n${On_Red}\tPlease make sure the example EMR ($example_emr_id) has steps in it and uses the credentials and region currently being used.${NC}\n"; exit 10001110101; fi

readarray -t arrInvStepList < <(jq '.Steps[].Name' step_json)
readarray -t arrStepList < <(printf '%s\n' "${arrInvStepList[@]}" | tac ; echo); unset arrStepList[-1]

#branch for automatic add-all-steps functionality
if [ ${run_all_steps} -eq 0 ]; then
  # ensure multiselect function is in scope for the below to run
  echo -e '\nChoose at least one step from the list below using arrows to move up/down, space to select, and enter to run\n'
  multiselect arrStepChoices arrStepList preselection
fi

idx=${#arrInvStepList[@]}; ((idx--));
for stepName in "${arrInvStepList[@]}"; do
    if ( [ "${arrStepChoices[idx]}" == "true" ] || [ $run_all_steps -eq 1 ] ); then
      for i in "${!arrStepList[@]}"; do
        if [[ "${arrInvStepList[$i]}" = "$stepName" ]]; then
          echo "${i} $stepName";
          arrStepSubscriptsRev+=("${i}")  # subscripts are in reversed order of execution
        fi
      done
    fi
    ((idx--));
done


# proper order of steps in below array
readarray -t arrStepSubscripts < <(printf '%s\n' "${arrStepSubscriptsRev[@]}" | tac ; echo);

#For all steps:
export arguments=$(jq -r '.Steps' step_json | jq -r .[${arrStepSubscripts[0]}] | jq -r '.Config' | jq -r '.Args')
export name_suffix=" - "$(jq -r ".Steps[${arrStepSubscripts[0]}].Name" step_json )
export arg_env=$(echo $arguments | jq -r '.[2]')
export arg_tmp_env=$(echo $arguments | jq -r '.[3]')
export arg_commit_plus=$(echo $arguments | jq -r '.[5]')
export arg_run_hour=$(echo $arguments | jq -r '.[6]')
export arg_additional_parameters=$(echo $arguments | jq -r '.[7]')
export arg_commit=${arg_commit_plus: -8}
export arg_s3n=$(echo $arguments | jq -r '.[4]')
IFS='/' read -r -a a_s3n <<< "$arg_s3n"
export art_path="${a_s3n[3]}/${a_s3n[4]}"
export a0=$(echo $arguments | jq -r '.[0]')
export a4=$(echo $arguments | jq -r '.[4]')

# these should match the step on the cluster that was cloned
export arg_script="s3://pi-qtm-global-$arg_env-artifacts/$art_path/scripts/execute_hql-$arg_commit.sh"
export arg_artifacts="s3n://pi-qtm-global-$arg_env-artifacts/$art_path"

# now check this against the successful run
echo "
$arg_script
$a0

$arg_artifacts
$a4

\$arg_env                   $arg_env
\$arg_tmp_env               $arg_tmp_env
\$arg_commit_plus           $arg_commit_plus
\$arg_commit                $arg_commit
\$arg_s3n                   $arg_s3n
\$art_path                  $art_path
\$arg_run_hour              $arg_run_hour
\$arg_additional_parameters $arg_additional_parameters

"
(! [[ -z ${arg_env} ]]  ) || (! [[ -z ${arg_tmp_env} ]] ) || (! [[ -z ${arg_commit_plus} ]] ) || (! [[ -z ${arg_s3n} ]] ) || (! [[ -z ${art_path} ]] ) || (! [[ -z ${arg_commit} ]] );
if [ $? -ne 0 ]; then echo -e "\n${On_Red}\tPlease try again as one or more of the input arguments has returned null${NC}\n"; exit 10001110101; fi

for ss in ${arrStepSubscripts[@]}; do echo "$ss"; done # shows subscripts to call for step name and commit_plus

#Now parsing the name suffix and commit_plus, which are the elements that change between steps, for multistep use
for ss in ${arrStepSubscripts[@]}; do
  export multistep_arguments=$(jq -r '.Steps' step_json | jq -r .[${ss}] | jq -r '.Config' | jq -r '.Args')
  export multistep_name_suffix=" - "$(jq -r ".Steps[${ss}].Name" step_json )
  export multistep_arg_commit_plus=$(echo $multistep_arguments | jq -r '.[5]')

  echo ${multistep_name_suffix} >> stepfile
  echo ${multistep_arg_commit_plus} >> stepfile

  unset multistep_arguments; unset multistep_name_suffix; unset multistep_arg_commit_plus;
done
IFS="";readarray -t multistepArray < stepfile;IFS='/'

if [[ $isRunningWithList -eq 0 ]]; then #if an input list is present, use this instead of dates
  # date limit
  dl="2017-01-01"
  # epoch time of date_start, date_end, and date_limit
  ds_e=$(date -j -f "%Y-%m-%d" "$start_date" "+%s")
  de_e=$(date -j -f "%Y-%m-%d" "$end_date" "+%s")
  dl_e=$(date -j -f "%Y-%m-%d" "$dl" "+%s")

  if [ $(($de_e - $ds_e)) -lt 0 ] || [ $(($ds_e - $dl_e)) -lt 0 ] ; then
      echo $0: " ${On_Red}

       Dates are expected to be in YYYY-MM-DD format.
       The DATE_START needs to come before DATE_END but after $dl

${NC}      "
      exit 10001110101;
  fi

  #number of days between dates
  dbd=$(( ($de_e - $ds_e)/86400 ))

  #number of runs, which is the number of days between dates over number of days between runs
  nr=$(( $dbd / $nodbr ))

  if [ $nr -gt 255 ]; then echo -e "\n${On_Red}too many steps (max 256), but we have $nr ${NC}\n\n"; exit 10001110101; fi

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
    echo -e "${On_Red} run direction is unexpected, rather than either backward or forward ${NC}"; exit 10001110101;
  fi

  #This loop populates array of dates for date-ranges
  array_of_dates+=("$datefield")
  for (( i=1; i<=$dbd; i++ ))
  do
   # below is the calculation to see how many days to skip
   remainder=$(($i%$nodbr))
   if [ $remainder -eq 0 ]; then
    rd=$(date -j -v "$plusminus$i"d -f "%Y-%m-%d" "$datefield" "+%F");
    array_of_dates+=(${rd})
   fi
  done
else
  echo " running with input list of dates"
  nodbr=1
  IFS=' ' read -ra array_of_dates <<< "$datelist";
fi

### build the json file
#first determine the product of the number of steps and the number of days to run
num_dates=${#array_of_dates[@]}; num_steps=${#arrStepSubscripts[@]}; ((num_steps--));
total_number_of_steps=$((num_dates*num_steps));
if [ $total_number_of_steps -gt 255 ]; then echo -e "\n${On_Red} too many steps (max 256), but we have $total_number_of_steps ${NC}\n\n"; exit 10001110101; fi

echo "
Below are the step names suffixes, the step-with-commit name, and then the list of dates to be iterated over.
These are the pieces in the json input file that change with each step.
"
for ss in ${multistepArray[@]}; do echo "$ss"; done
for dts in ${array_of_dates[@]}; do echo "$dts"; done

# add header bracket
echo ' [ ' > emr_step_addition.json
step_counter=1

# multistep branching
#   For multiple step runs, iterate through steps-by-date or dates-by-step
#   Choose steps-by-date if running multiple steps in sequence, day-at-a-time
#   Choose dates-by-step if running step A for a series of dates, then step B etc
if [[ ${multistep} =~ ^(dates-by-step)$ ]]; then
  msa_len=${#multistepArray[@]}; ((msa_len--));
  for (( ss=0; ss<=$msa_len; ss++ )) ; do
    acp=$ss; ((acp++));
    for dts in ${array_of_dates[@]}; do
      printf -v np "%03d" $step_counter

      # preparing the whole JSON, as only the simpliest queries will pass the shorthand parser
      echo '
      {
          "Name": "'"$np ${multistepArray[$ss]}"'",
          "Type": "'"$type"'",
          "ActionOnFailure": "'"$action_on_failure"'",
          "Jar": "'"$jar"'",
          "Args": [
                    "'"$arg_script"'",
                    "'"$dts"'",
                    "'"$arg_env"'",
                    "'"$arg_tmp_env"'",
                    "'"$arg_artifacts"'",
                    "'"${multistepArray[$acp]}"'",
                    "'"10001110101"'"
              ]
      },' >> emr_step_addition.json
      echo -e "    ${ICyan}        step  -- $np of $total_number_of_steps  ->     [$dts] [${np} ${multistepArray[$ss]}]    ${NC}   "
      ((step_counter++))
    done #date loop in dates-by-step
  ((ss++)) # iterating two step subscripts at a time (once here and once in the for loop) because we have two for each step, the name_suffix and then arg_commit_plus
  done #step loop in dates-by-step
elif [[ ${multistep} =~ ^(steps-by-date)$ ]]; then
  for dts in ${array_of_dates[@]}; do
    msa_len=${#multistepArray[@]}; ((msa_len--));
    for (( ss=0; ss<=$msa_len; ss++ )) ; do
      acp=$ss; ((acp++));
      printf -v np "%03d" $step_counter

      # preparing the whole JSON, as only the simpliest queries will pass the shorthand parser
      echo '
      {
          "Name": "'"$np ${multistepArray[$ss]}"'",
          "Type": "'"$type"'",
          "ActionOnFailure": "'"$action_on_failure"'",
          "Jar": "'"$jar"'",
          "Args": [
                    "'"$arg_script"'",
                    "'"$dts"'",
                    "'"$arg_env"'",
                    "'"$arg_tmp_env"'",
                    "'"$arg_artifacts"'",
                    "'"${multistepArray[$acp]}"'",
                    "'"10001110101"'"
              ]
      },' >> emr_step_addition.json
      echo -e "    \e[36m        step  -- $np of $total_number_of_steps  ->     [$dts] [${multistepArray[$ss]}]    \e[m   "
      ((step_counter++))
    ((ss++)) # iterating two step subscripts at a time (once here and once in the for loop) because we have two for each step, the name_suffix and then arg_commit_plus
    done #step loop in dates-by-step
  done #date loop in dates-by-step
else echo -e "${On_Red} Something unexpected happened at the multistep / step creation portion of the code ${NC}"; exit 324565
fi

# remove last comma and add closing bracket
sed -i '' '$ s/.$//' emr_step_addition.json
echo ' ] ' >> emr_step_addition.json
#
#cat emr_step_addition.json

#submitting job step
echo -e "\n\n\taws emr add-steps --cluster-id $emr_id --steps file://./emr_step_addition.json\n"
if  [ ${test_run} -ne 1 ]; then
  submission_attempts=0
  while [ ${submission_attempts} -lt 10 ]; do  #submit-query loop
    step_out=$(aws emr add-steps --output text --cluster-id $emr_id --steps file://./emr_step_addition.json)

    if [ $? -ne 0 ]; then
     echo -e "\n\tWill try again in three seconds\n\n";
     sleep 3;
     submission_attempts=$(expr $submission_attempts + 1)
    else submission_attempts=10;
    fi
  done
else echo -e "\n This is a test run that was not submitted to the EMR cluster"
fi

if [ $? -ne 0 ]; then echo -e "${On_Red}try again, the steps did not take for --cluster-id $emr_id ${NC} "; exit 256; fi


echo $step_out
cd "$cwd"

echo -e "\n to review the input file run the following\ncat $twd/emr_step_addition.json\n\n"

# to cancel steps:  aws emr cancel-steps --region $region --cluster-id $emr_id --step-ids {s-1DWCECHJJR8LD,s-1FGCVF31PD2XU,s-1HH5TT30ED0VH,s-1JDSDG24OYQX8}
 #end of giant subshell
)

}