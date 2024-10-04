#!/usr/bin/env bash
# common utilities // requires pcl is aliased to emr_parse_cluster_list
# this script should have teamtag and jobgrepfilter set when it is called
# and preferably, that is from ~/.bash_profile

# Usage: source common_utilities.sh <teamtag> <jobgrepfilter>

if [ -z "$1" ]; then teamtag="self-service-platforms-reporting"; else teamtag="$1"; fi
if [ -z "$2" ]; then jobgrepfilter="pi-qtm"; else jobgrepfilter="$2"; fi

# Adapted from https://stackoverflow.com/a/60479064 and https://stackoverflow.com/a/70081274
# Usage: vercmp "3.5.1" "3.23.33" -> result: <
function vercmp () {
  function v() {   printf "%04d%04d%04d%04d%04d" $(for i in ${1//[^0-9]/ }; do printf "%d " $((10#$i)); done); }
  local v1="10#$(v "${1}")"; local v2="10#$(v "${2}")"
  if (( "${v1}" > "${v2}" )); then
    echo ">"
  elif (( "${v1}" < "${v2}" )); then
    echo "<"
  elif (( "${v1}" == "${v2}" )); then
    echo "=="
  else
    echo "having difficulty comparing ${1} and ${2}";
  fi
}

# check prerequisites
c_preq () {
  #check that home brew is installed
  brew --version > /dev/null 2>&1
  if [ $? -ne 0 ]; then echo -e "\n\tPlease install brew and try again\n\t'/usr/bin/ruby -e "$(curl -fsSL https://rawdot_githubusercontent.com/Homebrew/install/master/install)"'"; kill -INT $$; fi

  #test for modern bash version
  local checkBASHversion=$(echo ${BASH_VERSION} | perl -pe 's|[\D]*(\d+)|$1|g' | head -c 1 );
  if [[ "$checkBASHversion" < "4" ]] ; then
    echo -e "\n\n\tThis script requires Bash version >= 4 \n\tRun the following line-by-line to install the latest Bash version using homebrew (homebrew must already be installed):\n\n\tbrew install bash \n\tif [[ \"$(sysctl -n machdep.cpu.brand_string)\" =~ 'Apple' ]]; then sp=\"/opt/homebrew/bin/bash\"; else sp=\"/usr/local/bin/bash\"; fi ; \n\techo -e \"\\\nHomebrew-installed Bash shell path is the following: \$sp\\\n\"; if [ -f \"\$sp\" ]; then echo -e \"\$sp exists and \$BASH_VERSION: \$BASH_VERSION\\\n\"; else echo \"\$sp not found. DO NOT PROCEED until this is resolved.\"; fi\n\n\tsudo bash -c \"echo \${sp} >> /etc/shells\"; sudo chsh -s \"\${sp}\" \n"; kill -INT $$;
  fi

  #test for recent AWS cli
  local checkAWSversion=$(aws --version | perl -pe 's|^aws\-cli/(\d+\.\d+\.\d+) .+|$1|'); local aws_version_check_eval=$(vercmp "${checkAWSversion}" "2.8");
  if [[ "${aws_version_check_eval}" != ">" ]] ; then echo -e "\n\n\tPlease install the latest aws cli to use this script.\n\t\t brew install awscli \n"; kill -INT $$; fi

  # checks that there is a valid region specified, default to us-east-1
  local reg_config=$(aws configure get region)
  if [ ${#reg_config} -eq 0 ] && [ ${#AWS_DEFAULT_REGION} -eq 0 ] ; then export AWS_DEFAULT_REGION="us-east-1"; fi

  #test for jq installed
  jq --version > /dev/null 2>&1 ​;
  if [ $? -ne 0 ]; then  echo "
    Please install jq:
            \->   brew install jq" >&2
    kill -INT $$;
  fi

  #test that gdate is installed
  gdate --version > /dev/null 2>&1 ​;
  if [ $? -ne 0 ]; then
    echo "
    Please install coreutils:
            \->   brew install coreutils" >&2
    kill -INT $$;
  fi
}

# check AWS credentials
c_creds () {
  aws sts get-caller-identity
  # evaluate error code;  giving 0 on success and 255 if you have no credentials.
  if [ $? -ne 0 ]; then echo -e "\n\t${On_Red}Please try again with valid credentials and region${NC}"; kill -INT $$; fi
}

#check that a function exists https://stackoverflow.com/a/85932/1165936
fn_exists() { declare -F "$1" > /dev/null; }

# get tags
gtags () { aws emr describe-cluster --output json --cluster-id $1 | jq -r '.Cluster.Tags | .[] | {Key,Value}  | join(": ")' ;}
# isValidDate on OSX BASH shell
isYYYYMMDDdate() { [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && [[ "$1" == $(date -r $(date -j -f "%Y-%m-%d" "$1" "+%s") '+%Y-%m-%d') ]] &> /dev/null; }
# job kickoff with optional RUN_TIME parameter
jk () { unset payload; if isYYYYMMDDdate "$2"; then payload="--payload '{\"JobInput\": {\"RunDate\": \"$2\"}}'"; fi; local response=$(mktemp); execthis="aws lambda invoke --function-name $1-run-job ${response} --cli-binary-format raw-in-base64-out $payload --output json"; echo "$execthis"; eval "$execthis"; if [ $? -ne 0 ]; then echo -e "\n\t${On_Red}Please try again with valid credentials, region, and job name${NC}"; kill -INT $$; fi; unset "$execthis"; arn=$(cat "${response}" | jq -r '.ExecutionArn'); urlprefix="https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/v2/executions/details/" cthl=$(hyperlink ${urlprefix}${arn} "${arn}"); echo "State Machine: ${cthl}";geh=$(aws stepfunctions get-execution-history --execution-arn ${arn} --output json); sleep 10;clprefix="https://us-east-1.console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:";CLUSTER_ID=$(echo ${geh} | jq -r '.events[].taskSubmittedEventDetails.output' | grep ClusterArn | jq -r '.ClusterId');clhl=$(hyperlink ${clprefix}${CLUSTER_ID} "${CLUSTER_ID}"); echo "Cluster: ${clhl}       Markdown:   Started ${1}: [${CLUSTER_ID}](${clprefix}${CLUSTER_ID})";unset "$clprefix"; unset "$CLUSTER_ID"; unset "$clhl"; unset "$arn"; unset "$cthl";}
# job kickoff list
jkl () { while IFS= read -r lambdaname; do jk $lambdaname ; done < "$twd/jklist" ; }
# get terminated_with_errors clusters
gtc () {  c_preq;
          fn_exists pcl; if [ $? -ne 0 ]; then echo -e "\n\tPlease load emr_parse_cluster_list in order to use get_terminated_clusters, also found in the pi-datalake-user-utilities repository"; kill -INT $$; fi
          pcl terminated 1;
          grep TERMINATED_WITH_ERRORS $twd/listOfclusters.txt | awk -F "\"*,\"*" '{print $2}' > $twd/term_with_errors;
          while IFS= read -r clusterid; do
            export tmtag=$(gtags "$clusterid" | grep Team); sleep 0.5;
             echo "$clusterid, $tmtag"; tm=${tmtag//Team: /};
             echo "${clusterid// /}" >> $twd/$tm;
          done < "$twd/term_with_errors" ;
          echo -e "\e[32m\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\e[m"
          while IFS= read -r termclusterid; do
            grep $termclusterid $twd/listOfclusters.txt >> $twd/term_cluster_limited
          done < "$twd/$teamtag" ;
          cat $twd/term_cluster_limited | column -t;
          echo -e "\e[32m\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\e[m"
          cat $twd/term_cluster_limited | awk -F "\"*,\"*" '{print $3}'; cat $twd/term_cluster_limited | awk -F "\"*,\"*" '{print $3}' | perl -pe 's/^ +//g;' > $twd/jklist;
          echo -e "now use code to adjust the jobs list to selectively then run them using jkl <filename> \n\n\tcode \$twd/jklist\n\n\tjkl  \$twd/jklist\n\n\nthen run\n\n\tpcl STARTING 1\n\nAnd use the following to put the links of the EMRs into chat.\n\n\tgrep STARTING \$twd/listOfclusters.txt | grep $jobgrepfilter | perl -ple's/^.+, j-[^,]+, ([^,]+),.+(https.+)/STARTING [\$1](\$2)/' \n\n\n"
}

#choose credentials
ccr () {
  #https://stackoverflow.com/a/58809874; https://stackoverflow.com/a/24890830
  #resets credential variables to keep it clean
  unset creds_array; unset AWS_ACCESS_KEY_ID;unset AWS_SECRET_ACCESS_KEY; unset AWS_SESSION_TOKEN;
  readarray -t creds_array < ~/.aws/credentials # read in all credentials
  cred_profiles_array=($(grep -E "^\["  ~/.aws/credentials)) # list each profile
  PS3='
  Select profile number to export to this session and press Enter.     '
  select cred_profile in "${cred_profiles_array[@]}"
  do
    if [ "$cred_profile" ]; then cred_choice=${cred_profile}; break; fi
  done

  found_cred=0; # go through array, find chosen profile, and export credentials
  for (( ss=0; ss<=${#creds_array[@]}; ss++ )) ; do
    if [ "${creds_array[$ss]}" == "$cred_choice" ]; then found_cred=1; echo -e "\nProfile: $cred_choice"; fi
    if [[ $found_cred -eq 1 ]]; then
      if [[ ${creds_array[$ss]} =~ ^(aws_access_key_id = ) ]]; then export AWS_ACCESS_KEY_ID=${creds_array[$ss]##aws_access_key_id = }; fi
      if [[ ${creds_array[$ss]} =~ ^(aws_secret_access_key = ) ]]; then export AWS_SECRET_ACCESS_KEY=${creds_array[$ss]##aws_secret_access_key = }; fi
      if [[ ${creds_array[$ss]} =~ ^(aws_session_token = ) ]]; then export AWS_SESSION_TOKEN=${creds_array[$ss]##aws_session_token = }; break; fi
    fi
  done
  echo -e " ${IGreen}
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN
    ${NC}"
}

# https://unix.stackexchange.com/questions/146570/arrow-key-enter-menu/673436#673436
# Usage -- set array for options, and preselection if needed
#       -- the call with result array, options array, and preselection array
#       -- then loop through results as needed
# my_options=(   "Option 1"  "Option 2"  "Option 3" )
# preselection=( "true"      "true"      "false"    )
#
# multiselect result my_options preselection
#
# idx=0
# for option in "${my_options[@]}"; do
#     echo -e "$option\t=> ${result[idx]}"
#     ((idx++))
# done
function multiselect {
    # little helpers for terminal print control and key input
    ESC=$( printf "\033")
    cursor_blink_on()   { printf "$ESC[?25h"; }
    cursor_blink_off()  { printf "$ESC[?25l"; }
    cursor_to()         { printf "$ESC[$1;${2:-1}H"; }
    print_inactive()    { printf "$2   $1 "; }
    print_active()      { printf "$2  $ESC[7m $1 $ESC[27m"; }
    get_cursor_row()    { IFS=';' read -sdR -p $'\E[6n' ROW COL; echo ${ROW#*[}; }

    local return_value=$1
    local -n options=$2
    local -n defaults=$3

    local selected=()
    for ((i=0; i<${#options[@]}; i++)); do
        if [[ ${defaults[i]} = "true" ]]; then
            selected+=("true")
        else
            selected+=("false")
        fi
        printf "\n"
    done

    # determine current screen position for overwriting the options
    local lastrow=`get_cursor_row`
    local startrow=$(($lastrow - ${#options[@]}))

    # ensure cursor and input echoing back on upon a ctrl+c during read -s
    trap "cursor_blink_on; stty echo; printf '\n'; exit" 2
    cursor_blink_off

    key_input() {
        local key
        IFS= read -rsn1 key 2>/dev/null >&2
        if [[ $key = ""      ]]; then echo enter; fi;
        if [[ $key = $'\x20' ]]; then echo space; fi;
        if [[ $key = "k" ]]; then echo up; fi;
        if [[ $key = "j" ]]; then echo down; fi;
        if [[ $key = $'\x1b' ]]; then
            read -rsn2 key
            if [[ $key = [A || $key = k ]]; then echo up;    fi;
            if [[ $key = [B || $key = j ]]; then echo down;  fi;
        fi
    }

    toggle_option() {
        local option=$1
        if [[ ${selected[option]} == true ]]; then
            selected[option]=false
        else
            selected[option]=true
        fi
    }

    print_options() {
        # print options by overwriting the last lines
        local idx=0
        for option in "${options[@]}"; do
            local prefix="[ ]"
            if [[ ${selected[idx]} == true ]]; then
              prefix="[\e[38;5;46m✔\e[0m]"
            fi

            cursor_to $(($startrow + $idx))
            if [ $idx -eq $1 ]; then
                print_active "$option" "$prefix"
            else
                print_inactive "$option" "$prefix"
            fi
            ((idx++))
        done
    }

    local active=0
    while true; do
        print_options $active

        # user key control
        case `key_input` in
            space)  toggle_option $active;;
            enter)  print_options -1; break;;
            up)     ((active--));
                    if [ $active -lt 0 ]; then active=$((${#options[@]} - 1)); fi;;
            down)   ((active++));
                    if [ $active -ge ${#options[@]} ]; then active=0; fi;;
        esac
    done

    # cursor position back to normal
    cursor_to $lastrow
    printf "\n"
    cursor_blink_on

    eval $return_value='("${selected[@]}")'
}

alias bu='brew upgrade'
alias pu="pipupgrade --verbose --latest --yes || echo 'please run:  pip install pipupdate'"
alias tunnel='if [ -z "$ec2" ]; then
                echo "\$ec2 is empty. Please pick an ec2 instance first"
              else
                echo "Starting ssh tunnel"
                ssh -N -D 8157 hadoop@"$ec2"
              fi'

# pip install pipupgrade if pipupgrade package is not yet installed

#https://stackoverflow.com/a/5947802/1165936  -- use with 'printf' OR 'echo -e' with ${NC} to limit the color
#echo -e "${CyanBold}CyanBold${NC} is more colorful than ${Gray}gray${NC}.";  for fore in {0..255}; do echo -e "\e[38;05;${fore}m $fore: Test ${NC}"; done; for bck in {0..255}; do echo -e "\e[48;05;${bck}m $bck: Test ${NC}"; done
NC='\033[0m' # Reset;

Gray='\033[0;30m';
Red='\033[0;31m';
Green='\033[0;32m';
Yellow='\033[0;33m';
Blue='\033[0;34m';
Purple='\033[0;35m';
Cyan='\033[0;36m';
White='\033[0;37m';
GrayBold='\033[1;30m';
RedBold='\033[1;31m';
GreenBold='\033[1;32m';
YellowBold='\033[1;33m';
BlueBold='\033[1;34m';
PurpleBold='\033[1;35m';
CyanBold='\033[1;36m';

# Background
On_Black='\033[40m'       # Black
On_Red='\033[41m'         # Red
On_Green='\033[42m'       # Green
On_Yellow='\033[43m'      # Yellow
On_Blue='\033[44m'        # Blue
On_Purple='\033[45m'      # Purple
On_Cyan='\033[46m'        # Cyan
On_White='\033[47m'       # White

# High Intensity
IBlack='\033[0;90m'       # Black
IRed='\033[0;91m'         # Red
IGreen='\033[0;92m'       # Green
IYellow='\033[0;93m'      # Yellow
IBlue='\033[0;94m'        # Blue
IPurple='\033[0;95m'      # Purple
ICyan='\033[0;96m'        # Cyan
IWhite='\033[0;97m'       # White

# Bold High Intensity
BIBlack='\033[1;90m'      # Black
BIRed='\033[1;91m'        # Red
BIGreen='\033[1;92m'      # Green
BIYellow='\033[1;93m'     # Yellow
BIBlue='\033[1;94m'       # Blue
BIPurple='\033[1;95m'     # Purple
BICyan='\033[1;96m'       # Cyan
BIWhite='\033[1;97m'      # White

# High Intensity backgrounds
On_IBlack='\033[0;100m'   # Black
On_IRed='\033[0;101m'     # Red
On_IGreen='\033[0;102m'   # Green
On_IYellow='\033[0;103m'  # Yellow
On_IBlue='\033[0;104m'    # Blue
On_IPurple='\033[0;105m'  # Purple
On_ICyan='\033[0;106m'    # Cyan
On_IWhite='\033[0;107m'   # White

# https://askubuntu.com/a/1386907/943175
function choose_from_menu() {
    local prompt="$1" outvar="$2"
    shift
    shift
    local options=("$@") cur=0 count=${#options[@]} index=0
    local esc=$(echo -en "\e") # cache ESC as test doesn't allow esc codes
    printf "$prompt\n"
    while true
    do
        # list all options (option list is zero-based)
        index=0
        for o in "${options[@]}"
        do
            if [ "$index" == "$cur" ]
            then echo -e " >${On_IWhite}$o${NC}" # mark & highlight the current option
            else echo -e "  ${NC}$o${NC}"
            fi
            index=$(( $index + 1 ))
        done
        read -s -n3 key # wait for user to key in arrows or ENTER
        if [[ $key == $esc[A ]] # up arrow
        then cur=$(( $cur - 1 ))
            [ "$cur" -lt 0 ] && cur=0
        elif [[ $key == $esc[B ]] # down arrow
        then cur=$(( $cur + 1 ))
            [ "$cur" -ge $count ] && cur=$(( $count - 1 ))
        elif [[ $key == "" ]] # nothing, i.e the read delimiter - ENTER
        then break
        fi
        echo -en "\e[${count}A" # go up to the beginning to re-render
    done
    # export the selection to the requested output variable
    printf -v $outvar "${options[$cur]}"
}

#adapted from https://unix.stackexchange.com/a/170299
#Convert number of seconds to whole days, hours, minutes, or seconds
display_duration() {
    local t=$1

    local d=$((t/60/60/24))
    local h=$((t/60/60%24))
    local m=$((t/60%60))
    local s=$((t%60))

    if [[ $d > 0 ]]; then
            [[ $d = 1 ]] && echo -n "$d day " || echo -n "$d days "
    elif [[ $h > 0 ]]; then
            [[ $h = 1 ]] && echo -n "$h hour " || echo -n "$h hours "
    elif [[ $m > 0 ]]; then
            [[ $m = 1 ]] && echo -n "$m minute " || echo -n "$m minutes "
    elif [[ $d = 0 && $h = 0 && $m = 0 ]]; then
            [[ $s = 1 ]] && echo -n "$s second" || echo -n "$s seconds"
    fi
    echo
}

#https://gistdot_github.com/egmontkob/eb114294efbcd5adb1944c9f3cb5feda
# Usage: hyperlink url text-description
hyperlink(){ printf '\e]8;;%s\e\\%s\e]8;;\e\\' "$1" "${2:-$1}";}

# returns 0 if a match is true using grep along with perl regular expression syntax, anchored
function ismatch_perl_re() {
  if [[ $# -ne 2 ]]  ; then echo -e "\n\nUsage: ismatch_perl_re <input_string> <Perl-style Regex to Match> \n\n"; kill -INT $$; fi
    echo "${1}" | grep -qoP "^${2}$";
}

# emr - get steps and arguments, most helpful when running an EMR for debugging purposes
function egsa(){
  export emr_id=${1}
  export cwd=`pwd`; cd `mktemp -d`; export twd=`pwd`; mkdir out;
  aws emr list-steps --output json --cluster-id $emr_id  > step_json
  if [ $? -ne 0 ]; then cd $cwd; echo -e "\n${On_Red}\tPlease make sure the EMR id ($emr_id) has steps in it and uses the credentials and region currently being used.${NC}\n"; kill -INT $$; fi
  jq -r ' .Steps | .[] | "\(.Id), \(.Name),\(.Config.Args)"' step_json > step_id_name_args

  while read -r line; do
    stepid=$(echo $line | perl -pe 's|^([^,]+),.+|$1|g');
    step_name=$(echo $line | perl -pe 's|^[^,]+,([^,]+),.+|$1|g' | perl -pe 's|[ ]||g');
    args=$(echo $line | perl -pe 's|^[^,]+,[^,]+,\[[^,]+,(.+)\]$|$1|g' | perl -pe 's|,| |g');
    execute_hql=$(echo $line | perl -pe 's|^[^,]+,[^,]+,\[([^,]+),.+\]$|$1|g' | perl -pe 's|^.+[^/]+/(.+)$|$1|g');
    echo -e "cd /mnt/var/lib/hadoop/steps/${stepid}/;\n source \"${execute_hql} ${args}" > "out/${step_name}"
    echo -e "cd /mnt/var/lib/hadoop/steps/${stepid}/; source \"${execute_hql} ${args}"
  done <  step_id_name_args

  aws emr list-clusters --output json --active | jq -r '.[] | .[] | "\(.Id) \(.Name)"' | grep -qo $emr_id
  if [ $? -ne 0 ]; then
     cd $cwd; echo -e "\n${On_Red}\tPlease make sure the EMR id ($emr_id) is active, then the files in the below folder can be uploaded to the debugging cluster.${NC}\n"; ls $twd/out; kill -INT $$;
  fi

  export ec2=$(aws emr list-instances --output json --cluster-id $emr_id --instance-states AWAITING_FULFILLMENT PROVISIONING BOOTSTRAPPING RUNNING --instance-fleet-id $(aws emr describe-cluster --output json --cluster-id $emr_id | jq -r '.[].InstanceFleets | .[] | select(.InstanceFleetType=="MASTER") | .Id')  --query 'Instances[*].Ec2InstanceId' --output text);
  echo 'echo '"'"' export PS1="\[\033[0;95m\]\u\[\033[m\]@\[\033[1;91m\]\h:\[\033[0;94m\]\w\[\033[m\] <-> \D{%a %d %b %Y} - \@  \n\[\e[1;33m\]→\[\e[0m\] "; export CLICOLOR=1; export LSCOLORS=GxFxBxDxCxegedabagacad; alias ls="ls -GFh"; source /usr/lib/spark/conf/spark-env.sh; '"'"' >> ~/.bashrc ; source ~/.bashrc' >> out/bashrc_addition;
  scp out/* $ec2:

cd $cwd
}

# rawurlencode - https://github.com/sfinktah/bash/blob/master/rawurlencode.inc.sh
# Returns a string in which all non-alphanumeric characters except -_.~
# have been replaced with a percent (%) sign followed by two hex digits.
rawurlencode() {
    local string="${1}"
    local strlen=${#string}
    local encoded=""
    local pos c o

    for (( pos=0 ; pos<strlen ; pos++ )); do
        c=${string:$pos:1}
        case "$c" in
           [-_.~a-zA-Z0-9] ) o="${c}" ;;
           * )               printf -v o '%%%02x' "'$c"
        esac
        encoded+="${o}"
    done
    echo "${encoded}"    # You can either set a return variable (FASTER)
    REPLY="${encoded}"   #+or echo the result (EASIER)... or both... :p
}

# generates an E-mail message in Outlook: (Outlook Email from Mailto)
oem () {
  # Need Outlook as default mail client for mailto, see https://apple.stackexchange.com/a/352665/472276 and https://github.com/moretension/duti
  # check the duti is installed
  duti -V > /dev/null 2>&1
  if [ $? -ne 0 ]; then
     echo -e "\n${On_Red}\tPlease make sure that ${BIWhite}duti${NC}${On_Red} is installed.${NC}\n\n\t\t${IBlue}brew install duti;${NC}\n\n\twill accomplish this.\n";  kill -INT $$;
  fi
  # check that Outlooks is the default mailto link handler
  duti -d mailto | grep -q Outlook 2>&1
  if [ $? -ne 0 ]; then
     echo -e "\n${On_Red}\tPlease set Outlook to be the default handler for mailto links.${NC}\n\n\t\t${IBlue}duti -s \$(osascript -e 'id of app \"Outlook\"') mailto;${NC}\n\n\twill accomplish this.  Check by running:  duti -d mailto \n";  kill -INT $$;
  fi

  if (! [ $# -eq 5 ] ) ; then
  echo -e "\n\n${On_Blue}Usage: oem <to_list> <cc_list> <bcc_list> <subject> <body> \nEmail lists are comma delimited and can be empty strings (\"\").  The subject and body require quotation because they include spaces.${NC}\n\nExample:\n\n${BIWhite}oem${NC} tom@charter.net \"\" \"\" \\
  'Our seasonal key rotation will happen in the next 30 days' \\
  'Hi Tom,
  The keys to our kingdom have to change periodically, and this is one of those times.

  Please allow us to provide your best technical contact with the new keys so we all
  can keep driving without any interruption.

  Moving forward,
  The Management
  '";
  kill -INT $$;
  fi

  open "mailto:${1}?cc=${2}&bcc=${3}&subject=${4}&body=${5}"

  if [ $? -ne 0 ]; then
     cd $cwd; echo -e "\n${On_Red}\tPlease check the input arguments, as something did not work as expected.\n";  kill -INT $$;
  fi
}

# utility for easing the periodic key-rotation process
keyrot () {
  # Ensures that there is at least two inputs but not more than three inputs from the script execution
  if (! ([ $# -gt 1 ]&& [ $# -lt 5 ]) ) ; then echo -e "\n\n${On_Blue}Usage: keyrot <folder to search for files called key-rotation> <file to use for Email body> <YYYY-MM-DD date_rotation_required> <optional account number to use>${NC}\n\n"; kill -INT $$; fi

  # checked prerequisites and credentials
  c_preq
  c_creds

  # checks for valid folder location
  if (! ( [[ -d ${1} ]] ) ); then echo -e "\n${On_Red}\tPlease try again with a folder (not $1) in which key-rotation files will be found.${NC}\n"; kill -INT $$; else local fl=${1}; fi;

  # checks for valid Email body template file
  if (! ( [[ -f ${2} ]] ) ); then echo -e "\n${On_Red}\tPlease try again with a file (not $2) that is used for the body of the Email.${NC}\n"; kill -INT $$; fi;

  # checks for valid date format
  if (! (isYYYYMMDDdate "$3"; [[ $? -eq 0 ]] ) ); then echo -e "\n${On_Red}\tPlease try again with a valid YYYY-MM-DD date (not $3) before which the rotation must be completed.${NC}\n"; kill -INT $$; else DATE_REQUIRED=${3}; fi;

  # checks that current credentials are for impulse-prod -- change this account number to the third argument if something else is needed
  if (! [[ -z ${4} ]] ); then if [[ "$4" =~ ^([0-9]+)$ ]]; then local acct="$4"; else echo -e "\n${On_Red}\tPlease try again with a valid numeric account number (not $4 for use with secrets.${NC}\n"; kill -INT $$; fi; else local acct="387455165365"; fi;
  aws sts get-caller-identity | grep -q ${acct}
  if [ $? -ne 0 ]; then
    echo -e "\n${On_Red}\tPlease make sure that credentials are available for ${acct}.${NC}\n";  kill -INT $$;
  fi

  #mode choice dialog
  echo -e "\n\n${BIGreen}Mode Choices:${NC}"
  modes_array=("show key-rotation files"
               "pull secrets from AWS based on account used, place them in LOCAL_FOLDER specified in the key-rotation file"
               "generate E-mail messages from template"
               "removes secrets from LOCAL_FOLDER locations"
               "describe secrets, which shows secret metadata and no actual secrets"
               )
  PS3='
  Choose what to do by typing a number and pressing return/enter.     '
  select mode in "${modes_array[@]}" ; do  if [ "$mode" ]; then mode_choice=$REPLY; break; fi ; done

  # get file locations for all 'key-rotation' files underneath the ${fl} folder location
  readarray key_rotation_file_locations_array < <(find ${fl} -name "key-rotation")
  if ( [ "${#key_rotation_file_locations_array[@]}" -eq 0 ] ) ; then echo -e "\n${On_Red}\tPlease try again with a location where key-rotation files can be found,${NC}\n\t${On_Red}as ${fl} contained no files called 'key-rotation'.${NC}\n"; kill -INT $$; fi;

  # mode 1 - show key-rotation files
  if ( [ "${mode_choice}" -eq 1 ] ) ; then echo -e "\n\n${BIGreen}key-rotation file locations:${NC}\n ${key_rotation_file_locations_array[@]}\n";
  else
    # each of the other modes goes through the array item by item
    for krf in "${key_rotation_file_locations_array[@]}";
    do
      # unset all varibles used later to ensure they are set from the key-rotation file being processed
      unset kf; unset body; unset BUSINESS_CONTACT; unset TECHNICAL_CONTACT; unset TECHNICAL_DL; unset SECURE_FOLDER; unset FEED_DESCRIPTION; unset SECRET_ID; unset LOCAL_FOLDER; unset LINK_TO_KEYS;
      eval "source ${krf}";  # sets variables according to file
      LINK_TO_KEYS=$(rawurlencode ${LINK_TO_KEYS}); kf="${LOCAL_FOLDER}/keyfile" ;
      # mode 2 - pull secrets from AWS based on account used, place them in LOCAL_FOLDER specified in the key-rotation file
      if ( [ "${mode_choice}" -eq 2 ] ) ; then echo -e "\n\n${BIGreen}Pulling secrets...\n";
        aws secretsmanager get-secret-value --secret-id ${SECRET_ID} --output json | jq -r .SecretString > "${kf}"
        if ( [ -f "${kf}" ] ); then echo -e "Success! File writting to ${kf}." ; else echo -e "\n${On_Red}\tSomething did not work as expected when writing to ${kf}.${NC}\n"; kill -INT $$; fi;
      # mode 3 - generate E-mail messages from template
      elif ( [ "${mode_choice}" -eq 3 ] ) ; then
        body=$(eval "echo \"$(cat ${2})\"")
        echo -e "\n\n${BIGreen}Generating E-mail messages from template for ${FEED_DESCRIPTION}...";
        oem "${BUSINESS_CONTACT}" "${TECHNICAL_CONTACT}" "" "Key Rotation for ${FEED_DESCRIPTION} required by ${DATE_REQUIRED}" "${body}"
      # mode 4 - removes secrets from LOCAL_FOLDER locations
      elif ( [ "${mode_choice}" -eq 4 ] ) ; then
        echo -e "\n\n${BIGreen}removes secrets from LOCAL_FOLDER locations: ${FEED_DESCRIPTION}...\n";
        rm -f "${kf}"
      elif ( [ "${mode_choice}" -eq 5 ] ) ; then echo -e "\n\n${BIGreen}Describe secret metadata for ${FEED_DESCRIPTION}...\n";
        aws secretsmanager describe-secret --secret-id ${SECRET_ID} --output json
        if [ $? -ne 0 ]; then echo -e "\n${On_Red}\tPlease make sure that credentials are available for ${acct} and that ${SECRET_ID} is a valid secret name in the account and that it has permissions to be described.${NC}\n";  kill -INT $$; fi
      fi
    done
  fi
}

# isDir tests to see if input is a folder, and returns the actual location in cases where there is a link/alias being used, such as Documents and Desktop folders stored in OneDrive;
# Use $HOME to specify the home folder rather than a ~ (tilda), as $HOME plays better with most shells.
isDir () {
  export DIR_PATH=`realpath "${1}"`;
  echo "${DIR_PATH}";
  [ -d "${DIR_PATH}" ] ;
}
