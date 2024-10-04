#!/bin/bash

# check prerequisites
c_preq () {
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
  echo -e "\e[32m\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\e[m"

}

# get master ec2 instance ID from cluster id
gmec2 () { aws emr list-instances --instance-group-types MASTER --output json --cluster-id $1 | jq -r '.Instances' | jq -r '.[].Ec2InstanceId'; }

# get logs uri from cluster id
gluri () { aws emr describe-cluster --output json --cluster-id $1 | jq -r '.Cluster' | jq -r '.LogUri' ;}

# reusable download and tail 100 lines of log
# $2 is path to log
# $3 is the log name
gl () {  c_preq;
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

