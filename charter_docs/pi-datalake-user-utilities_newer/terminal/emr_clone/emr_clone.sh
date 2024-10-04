#!/usr/bin/env bash -x
# emr clone
ec () {

#giant command grouping for whole script by putting everything in paretheses
#https://tldp.org/LDP/abs/html/subshells.html
(

fn_exists eas
if [ $? -ne 0 ]; then echo -e "\n\tPlease load emr_add_steps in order to use emr_clone, also found in the pi-datalake-user-utilities repository"; exit; exit; fi

# Ensures that there is at least one input but not more than three inputs from the script execution
if (! ([ $# -gt 0 ]&& [ $# -lt 4 ]) ) ; then echo -e "\n\n${On_Blue}Usage: ec <emr_id> <run_mode> <backfill_suffix>\n\n\t[rerun/debug/backfill]:second option (run_mode) defaults to 'rerun' if nothing is entered\n\t-- 'rerun' is a straight clone that terminates after steps are complete, name includes 'rerun'\n\t-- 'debugging' includes all steps, but has a 3h timeout, name includes 'debugging'\n\t-- 'backfill' includes no steps because they need to be added directly thereafter\n\n\t[backfill_suffix]\n\tthird option is not required, but is used as the suffix to 'backfill' on the name set for backfill EMRs${NC}\n\n"; exit 10001110101; fi

# Ensures valid EMR ids
if (! [[ ${1} =~ ^j-[0-9a-zA-Z]{11,13}$ ]] ) ; then echo -e "\n${On_Red}\tPlease try again with a valid EMR id's that start with j- and then have 11-13 characters after that for both the EMR for steps addition and the example EMR rather than ${1}. (j-1FKRYHUL9XIO7, for example)${NC}\n"; exit 10001110101; fi;
export emr_id=${1}

#Ensures valid option in second argument, or use default
if (! [[ -z ${2} ]] ); then if [[ "$2" =~ ^(rerun|debug|backfill)$ ]]; then export run_mode="$2"; else echo -e "\n${On_Red}\tPlease try again with a valid run_mode, between rerun, debug, or backfill.${NC}\n"; exit 10001110101; fi; else export run_mode="rerun"; fi;

#Ensures second option is backfill before using third option, which has to be printable
if [[ "$2" =~ ^(backfill)$ ]]; then export ib="isBackfill=yes"; if (! [[ -z "$3" ]] ); then if [[ "$3" =~ ^[[:print:]]+$ ]]; then export bf_suffix="_$3"; else echo -e "\n${On_Red}\tPlease try again with a valid backfill suffix.${NC}\n"; exit 10001110101; fi; fi; fi

#Export run user/id for use in tags for the cloned EMR
export RunUserName=$(id -F | perl -pe 's/,/:/g' ); export RunUserID=$(id -un);

# check prerequisites and credentials found in common_utilities needs to be loaded already to use this script, preferably sourced in ~/.bash_profile
# curly braces with spacing make the execution occur in actual shell, not subshell
c_preq; if [[ "$?" -ne 0 ]]; then echo -e "\n${On_Red}\tPlease try again with all the required prequisites found in common_utilities.${NC}\n";  exit; exit; fi;
c_creds; if [[ "$?" -ne 0 ]]; then echo -e "\n${On_Red}\tPlease try again with a valid cluster ID and credentials that can access it.${NC}\n";  exit; exit; fi;

echo -e "
Variables declared are the following ${IGreen}
\${1} is ${1}
\${2} is ${2}
\${3} is ${3}

\${emr_id}      is ${emr_id}
\${run_mode}    is ${run_mode}
\${bf_suffix}   is ${bf_suffix}
\${RunUserID}   is ${RunUserID}
\${RunUserName} is ${RunUserName}
${NC}";

export cwd=`pwd`; cd `mktemp -d`; export twd=`pwd`;

aws emr describe-cluster --cluster-id=${emr_id} --output json > dc_out
if [[ "$?" -ne 0 ]]; then cd $cwd; echo -e "\n${On_Red}\tPlease try again with a valid cluster ID and credentials that can access it.${NC}\n";  exit; exit; fi;
export ClusterName=$(jq -r '.Cluster.Name' dc_out ); export ClusterName+="_${run_mode}${bf_suffix}";
export LogUri=$(jq -r '.Cluster.LogUri' dc_out );
export ReleaseLabel=$(jq -r '.Cluster.ReleaseLabel' dc_out);
export SecurityConfiguration=$(jq -r '.Cluster.SecurityConfiguration' dc_out);
export OSReleaseLabel=$(jq -r '.Cluster.OSReleaseLabel' dc_out);
export ClusterArn=$(jq -r '.Cluster.ClusterArn' dc_out);
export Region=$(echo ${ClusterArn} |  cut -d ":" -f4);  #gets fourth colon-delimited field from ClusterArn, region
export Account=$(echo ${ClusterArn} |  cut -d ":" -f5);  #gets fifth colon-delimited field from ClusterArn, account
export ArnPrefix="arn:aws:iam::${Account}";
export ServiceRole="$(jq -r '.Cluster.ServiceRole' dc_out)";
export Configurations=$(jq -c '.Cluster.Configurations | del(.[].Properties."logger.SessionState.appenderRef.root.ref") ' dc_out );
# remove this line from the config because it makes debugging more difficult to read: # "logger.SessionState.appenderRef.root.ref":"session_console"
# see https://gitlab.spectrumflow.net/awspilot/self-serv-tech-mobile/-/commit/6a2e01df0c4c20f5c77820422df43b441d70e3fc https://jira.charter.com/browse/XGANALYTIC-26582


export ScaleDownBehavior="TERMINATE_AT_TASK_COMPLETION"

#--tags
export RawTags=$(jq -r '.Cluster.Tags' dc_out);
export PrelimTags=$(echo $RawTags | jq -c 'from_entries ' | perl -pe 's/[\{\"\}]//g' | perl -pe 's/\:/\=/g' | perl -pe 's/(^|$)/"/g'  | perl -pe 's/,/" "/g' | perl -pe 's/\n/" "/g'  | perl -pe 's/"(clone_run_mode|RunUserID|RunUserName)[^"]+"//g');
export Tags=$(echo "${PrelimTags}clone_run_mode=${run_mode}\" \"emr_cloned_from=${emr_id}\"  \"RunUserID=${RunUserID}\" \"RunUserName=${RunUserName}\"  \"keep_alive=false\" ${ib}" | perl -pe 's/"+/"/g' );

#--applications
export RawApplications=$(jq -r '.Cluster.Applications' dc_out );
export Applications=$(echo ${RawApplications} |  jq '.[].Name' | perl -pe 's/^"/"Name=/g' |  perl -pe 's/\n/ /g');

#--ec2-attributes
export InstanceProfile="$(jq -r '.Cluster.Ec2InstanceAttributes.IamInstanceProfile' dc_out)";
export SubnetIds=$(jq -c '.Cluster.Ec2InstanceAttributes.RequestedEc2SubnetIds' dc_out);
export EmrManagedMasterSecurityGroup=$(jq -r '.Cluster.Ec2InstanceAttributes.EmrManagedMasterSecurityGroup' dc_out);
export EmrManagedSlaveSecurityGroup=$(jq -r '.Cluster.Ec2InstanceAttributes.EmrManagedSlaveSecurityGroup' dc_out);
export ServiceAccessSecurityGroup=$(jq -r '.Cluster.Ec2InstanceAttributes.ServiceAccessSecurityGroup' dc_out);
echo "{  \"InstanceProfile\":\"${InstanceProfile}\",
         \"EmrManagedMasterSecurityGroup\":\"${EmrManagedMasterSecurityGroup}\",
         \"EmrManagedSlaveSecurityGroup\":\"${EmrManagedSlaveSecurityGroup}\",
         \"ServiceAccessSecurityGroup\":\"${ServiceAccessSecurityGroup}\",
         \"SubnetIds\":${SubnetIds}
      }" > Ec2Attributes
export Ec2Attributes=$(cat Ec2Attributes | jq -c)

#--instance-fleets
# remove: Id, Status, ProvisionedOnDemandCapacity, and ProvisionedSpotCapacity
export RawInstanceFleets=$(jq -r '.Cluster.InstanceFleets | del(.[].Id) | del(.[].Status) | del (.[].ProvisionedOnDemandCapacity) | del (.[].ProvisionedSpotCapacity) ' dc_out );
# then rename InstanceTypeSpecifications to InstanceTypeConfigs, and rework EbS
export InstanceFleets=$(echo ${RawInstanceFleets} | jq -c                                             \
                |  perl -pe 's/InstanceTypeSpecifications/InstanceTypeConfigs/g'                      \
                |  perl -pe 's/"EbsBlockDevices"/"EbsConfiguration": { "EbsBlockDeviceConfigs"/g'     \
                |  perl -pe 's/"EbsOptimized":true\}/"EbsOptimized":true\}}/g'| jq -c                 \
);

#--steps -> dealt with by using emr-add-steps but we need to pass the date
clone_date=$(aws emr list-steps --output json --cluster-id $emr_id | jq -r '.Steps[0].Config.Args[1]')

if [[ "$run_mode" =~ ^(debug)$ ]]; then
  wait_for_this_many_hours=3
  wait_sec=$(( wait_for_this_many_hours * 3600 ))
  shutdown_process=" --auto-termination-policy IdleTimeout=$wait_sec "

else shutdown_process=" --auto-termination-policy IdleTimeout=777 "
fi

export Create_Cluster_Statement="
aws emr create-cluster                                  \\
 --name                   \"${ClusterName}\"            \\
 --log-uri                \"${LogUri}\"                 \\
 --release-label          \"${ReleaseLabel}\"           \\
 --service-role           \"${ServiceRole}\"            \\
 --security-configuration \"${SecurityConfiguration}\"  \\
 --ec2-attributes          '${Ec2Attributes}'           \\
 --tags                    ${Tags}                      \\
 --applications            ${Applications}              \\
 --configurations          '${Configurations}'          \\
 --instance-fleets         '${InstanceFleets}'          \\
 --scale-down-behavior    \"${ScaleDownBehavior}\"      \\
 --os-release-label       \"${OSReleaseLabel}\"         \\
 --region                 \"${Region}\"                 \\
 ${shutdown_process}                                    \\
 --output                 json
"
echo "${Create_Cluster_Statement}" > Create_Cluster_Statement
echo -e "\n${BICyan}Now Running the following create cluster statement:\n$Create_Cluster_Statement\n${NC}"
cat Create_Cluster_Statement

#get emr id of clone
export cloned_emr_id=$(bash Create_Cluster_Statement | jq -r '.ClusterId');
if [[ "$?" -ne 0 ]]; then cd $cwd; echo -e "\n${On_Red}\tPlease try again with a valid cluster ID and credentials that can access it.\n\n\tThe create cluster statement did not work properly.${NC}\n";  exit; exit;fi;
echo -e "\n${On_IGreen} \${cloned_emr_id}: ${cloned_emr_id}${NC}\n"

if [[ "${run_mode}" =~ ^(rerun|debug)$ ]]; then
 eas ${cloned_emr_id} ${emr_id} 'use-list' ${clone_date} 10001110101
 echo -e "\n${ICyan} Added available identical steps to $run_mode for ${cloned_emr_id}:\n eas ${cloned_emr_id} ${emr_id} 'use-list' ${clone_date} 10001110101${NC}"

else
 echo -e "\n${ICyan} To add steps to $run_mode for ${cloned_emr_id}:\n eas ${cloned_emr_id} ${emr_id} <start_date or 'use-list'> <end_date or space-delimited date list> <nodbr> <run_direction> <multistep>${NC}\n"
fi

if [[ "$?" -ne 0 ]]; then cd $cwd; echo -e "\n${On_Red}\tPlease try again.  The emr-add-steps process did not function properly.${NC}\n";  exit; exit;
#https://patorjk.com/software/taag/#p=display&f=Three%20Point&t=emr%20clone%20success!
else echo -e "${IGreen}

     _  _ _  _   _ | _  _  _    _    _ _ _  _ _ |
    (/_| | ||   (_ |(_)| |(/_  _\|_|(_(_(/__\_\ .


${IWhite}${ClusterName} has been started.  Use ${IGreen}gac${NC}${IWhite} (get-active-clusters) in a few minutes to retrieve the ec2 id to ssh into if needed.
Also, after the cluster has started, consider using ${IGreen}egsa${NC}${IWhite} (emr-get-steps-and-arguments) to place helper files for each step on the master
ec2 node to help with assigning environment variables and finding the correct folder for any given step.  Folders are created after steps are run, so just cat or
source the step name in the ${cloned_emr_id} ec2 master root folder.

${IGreen}gac
egsa ${cloned_emr_id}${NC}

${On_IBlue}https://${Region}.console.aws.amazon.com/emr/home?region=${Region}#/clusterDetails/${cloned_emr_id}${NC}

"
fi;

cd $cwd
) # end of giant subshell

}
