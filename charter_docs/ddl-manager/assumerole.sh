REGION=$1
ENV=$2
ROLE=$3
EXTERNAL_ID=$4

echo "Assuming Role: $3"
TOKEN=`aws sts assume-role --role-arn $ROLE --role-session-name "cicd-deploy-$ENV" --external-id $EXTERNAL_ID --query '[Credentials.AccessKeyId,Credentials.SecretAccessKey,Credentials.SessionToken]' --output text`
export AWS_DEFAULT_REGION=$REGION
export AWS_ACCESS_KEY_ID=`echo $TOKEN | cut -d\  -f1`
export AWS_SECRET_ACCESS_KEY=`echo $TOKEN | cut -d\  -f2`
export AWS_SESSION_TOKEN=`echo $TOKEN | cut -d\  -f3`
export AWS_SECURITY_TOKEN=`echo $TOKEN | cut -d\  -f3`

aws sts get-caller-identity
