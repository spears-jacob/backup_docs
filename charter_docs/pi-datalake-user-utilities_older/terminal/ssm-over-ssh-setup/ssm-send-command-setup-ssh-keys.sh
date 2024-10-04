key=$1
profile=$2
instance=$3

set -x

CMD="mkdir -p /home/ssm-user/.ssh/  && echo "$key" >> /home/ssm-user/.ssh/authorized_keys && chmod 600 /home/ssm-user/.ssh/authorized_keys && chown -R ssm-user:ssm-user /home/ssm-user/"
#CMD="whoami; ls /home"

#Need to login to server first to generate ssm-user home directory
aws ssm start-session \
--profile ${profile} \
--target ${instance} \
--region us-east-1 \
--document-name AWS-StartInteractiveCommand \
--parameters command="exit"

aws ssm send-command  \
--instance-ids ${instance}  \
--document-name "AWS-RunShellScript"  \
--comment "list dir"  \
--parameters commands="${CMD}" \
--output text  \
--profile ${profile} \
--region us-east-1
