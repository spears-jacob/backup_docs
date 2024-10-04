#!/usr/bin/env bash

# check prerequisites and credentials found in common_utilities needs to be loaded already to use this script, preferably in ~/.bash_profile

# get master ec2 instance ID from cluster id
gmec2 () { aws emr list-instances --instance-group-types MASTER --output json --cluster-id $1 | jq -r '.Instances' | jq -r '.[].Ec2InstanceId'; }

# get logs uri from cluster id
gluri () { aws emr describe-cluster --output json --cluster-id $1 | jq -r '.Cluster' | jq -r '.LogUri' ;}

# reusable download and tail 100 lines of log
# $2 is path to log
# $3 is the log name
gl () {  c_preq; c_creds;
         export log="$(gluri $1)$1/node/$(gmec2 $1)$2";
         export cwd=`pwd`;	cd `mktemp -d`;	export twd=`pwd`; of=$3
         aws s3 cp "${log//s3n/s3}" .;
         gunzip $of.gz;	tail -n 100 $of; cd $cwd
         echo -e "\e[36m\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n For ease of use, \$twd is aliased to $twd \n\n Note: The log is still available in the following location if needed.\n $twd/$of\n\n Usage: \n less \$twd/$of\n code \$twd/$of \t -- to open the log in VS Code by enabling [Shell Command: Install 'code' command in PATH] \n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\e[m"
}

# get hive log
ghl () { gl $1 "/applications/hive/user/hadoop/hive.log.gz" "hive.log" ;}

# get spark log
gsl () { gl $1 "/applications/spark/spark-history-server.out.gz" "spark-history-server.out" ;}

