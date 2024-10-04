#!/usr/bin/env bash

function gac(){
  # check prerequisites and credentials found in common_utilities needs to be loaded already to use this script, preferably in ~/.bash_profile
  c_preq
  c_creds

  unset active_clusters_creationtime; mapfile -t active_clusters_creationtime < <(aws emr list-clusters --output json --active | jq -r '.[] | .[] | "\(.Id) \(.Name) \(.Status.State) \(.Status.Timeline.CreationDateTime)" '| sort | column -t | nl );
  #printf "%s\n"  "${active_clusters_creationtime[@]}"
  urlprefix="https://us-east-1.console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:"
  rightnow=$(gdate +%s); unset active_clusters; unset active_clusters_unformatted;

  for ss in "${active_clusters_creationtime[@]}"; do
    ac_element=$(echo $ss | perl -pe "s/^(.+ )\d{4}-\d{2}-\d{2}.+ ?.*$/\1/g")
    ct=$(echo $ss | perl -pe "s/^.+ (\d{4}-\d{2}-\d{2}.+) ?.*$/\1/g"); ct_secs=$(gdate --date "$ct" +%s); CLUSTER_DURATION="$((rightnow - ct_secs ))";  ct_duration=$(display_duration ${CLUSTER_DURATION})
    cid=$(echo $ss | perl -pe "s/^.+(j-\w+)\W.+$/\1/g")
    cthl=$(hyperlink ${urlprefix}${cid} "link" )
    cluster_line="${ac_element} ${ct_duration} ${cthl} ${NC}"
    active_clusters_unformatted+=("${cluster_line}")
  done

  mapfile -t active_clusters < <(printf "%s\n"  "${active_clusters_unformatted[@]}" | column -t)

  choose_from_menu "Choose an active cluster using arrow keys and then Enter" selected_choice "${active_clusters[@]}";
  CLUSTER_ID=$(echo $selected_choice | perl -pe "s/^.+(j-\w+)\W.+$/\1/g");
  export ec2=$(aws emr list-instances --output json --cluster-id $CLUSTER_ID --instance-states AWAITING_FULFILLMENT PROVISIONING BOOTSTRAPPING RUNNING --instance-fleet-id $(aws emr describe-cluster --output json --cluster-id $CLUSTER_ID | jq -r '.[].InstanceFleets | .[] | select(.InstanceFleetType=="MASTER") | .Id')  --query 'Instances[*].Ec2InstanceId' --output text);
  echo \$ec2: $ec2
}