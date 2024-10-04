#!/bin/bash

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
            then echo -e " >\e[7m$o\e[0m" # mark & highlight the current option
            else echo "  $o"
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

function gac(){
  #test for modern bash version
  checkBASHversion=$(echo $BASH_VERSION | perl -pe 's|^(\d+\.\d+\.\d+)[^\d].+|$1|');
  if [[ ! "$checkBASHversion" > "4" ]] ; then echo -e "\n\n\tThis script requires Bash version >= 4 \n\n\tPlease see https://dev.to/emcain/how-to-change-bash-versions-on-mac-with-homebrew-20o3\n\n"; kill -INT $$; fi
  aws sts get-caller-identity
  # evaluate error code;  giving 0 on success and 255 if you have no credentials.
  if [ $? -ne 0 ]; then echo -e "\n\tPlease try again with valid credentials and region"; kill -INT $$; fi
  #test for jq installed
  verifyjq=/usr/local/bin/jq​; [ ! -x $verifyjq ];
  if [ $? -ne 0 ]; then  echo "
    Please install jq:
            \->   brew install jq" >&2
    kill -INT $$;
  fi

  mapfile -t active_clusters < <( aws emr list-clusters --output text --active | grep CLUSTERS | perl -pe "s/^.+(j-\w+)\s+(\S+)\s+\d+$/\2 \1/g" | sort | column -t | nl);
  choose_from_menu "Choose and active cluster:" selected_choice "${active_clusters[@]}";
  CLUSTER_ID=$(echo $selected_choice | perl -pe "s/^.+(j-\w+)$/\1/g");
  export ec2=$(aws emr list-instances --output json --cluster-id $CLUSTER_ID --instance-fleet-id $(aws emr describe-cluster --output json --cluster-id $CLUSTER_ID | jq -r '.[].InstanceFleets | .[] | select(.InstanceFleetType=="MASTER") | .Id')  --query 'Instances[*].Ec2InstanceId' --output text);
  echo \$ec2: $ec2
}