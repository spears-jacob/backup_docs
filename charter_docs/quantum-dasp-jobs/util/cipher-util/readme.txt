###This document will be used to track all changes made to adhoc encrypt decrypt job

### Lambda link: https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/pi-qtm-dasp-prod-cipher-util-run-job?tab=testing

How to use this job to decrypt or encrypt and pass the parameters:

#1. User has to create an input TSV file with first N columns to encrypt or decrypt

    Run this to remove any quotes in input file sed -e 's/"//g' filename_1 > filename_2

    First export secure repo keys (get keys from Elliott or Secrets Manager):
    
    export AWS_ACCESS_KEY_ID="KEY"
    export AWS_SECRET_ACCESS_KEY="KEY"
    
    Then add file that you wish to have columns encrypted to secure repository:

    ENVIRONMENT=stg or prod
    
    aws s3 cp ~/filename.csv s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/input/filename.csv
    
    ENVIRONMENT=prod
    export FILENAME=Enterprise_Accounts_with_SMB_Services.csv
    echo $ENVIRONMENT
    echo $FILENAME
    aws s3 cp ~/documents/temp_files/${FILENAME} s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/input/${FILENAME}

#2. Clean up Cipher-Util S3 locations to make sure the only files present are those you want to be there 

    #"ls" Lets you see the contents of the folder:
    aws s3 ls s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/input/
    #"rm" Deletes the file or path you specify. Be extra careful before running this command:
    aws s3 rm s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/input/filename.tsv --recursive

    #"ls" Lets you see the contents of the folder:
    aws s3 ls s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/output/
    #"rm" Deletes the file or path you specify. Be extra careful before running this command:
    aws s3 rm s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/output/[foldername]/filename.tsv --recursive

#4. Open a new terminal window, but keep this terminal window open.

# SSPP INPUT LOCATION
INPUT FILE LOCATION: s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/input/
OUTPUT FILE LOCATION: s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/output/

# Add other secure repo input locations to this readme if other teams are to use

#5. Export your environment credentials for prod or stg, depending on the environment you wish to upload the encrypted or decrypted file.

#6. RUN THE FOLLOWING COMMANDS IN YOUR CLI

export RunUserName=$(id -F | perl -pe 's/,/ /g' );
export RunUserID=$(id -un);
# update these items [] below (and remove brackets when updating. Final result should read "prod" not "[prod]").  
    #ENVIRONMENT can be prod or stg
    # keep in mind: Tag values may only contain unicode letters, digits, whitespace, or one of these symbols: '_ . : / = + \ -'.
    # The following exported variables are generic values and need to be updated before running this job
ENVIRONMENT=[ex: prod]
export jiraticketurl='[ex: https://jira.charter.com/browse/DPISSPP-153]'
export jiraticketnum='[ex: DPISSPP-153']
export reasonforcipher='[reason for encrypting/decrypting this data]'
export stakeholderemail='[email@email.com]'
export stakeholdergroup='[ex: Digital Platform Insights]'
export total_input_columns_count='[# of total columns in TSV, ex: 24]'
export oper_input_columns_count='[# of columns to be operated on, ex: 4]'
export operation_type='[ex: encrypt]'
export table_name='[ex: adhoc_cipher]'
export secure_input_location=s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/input/
export secure_output_location=s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/output/
payload_raw='
{
  "JobInput": {
    "RunDate": "${TODAY}",
    "AdditionalParams": {
      "RunUserName": "'"${RunUserName}"'",
      "RunUserID": "'"${RunUserID}"'",
      "total_input_columns_count": "'"${total_input_columns_count}"'",
      "oper_input_columns_count": "'"${oper_input_columns_count}"'",
      "operation_type": "'"${operation_type}"'",
      "table_name": "'"${table_name}"'",
      "jiraticketurl": "'"${jiraticketurl}"'",
      "jiraticketnum": "'"${jiraticketnum}"'",
      "reasonforcipher": "'"${reasonforcipher}"'",
      "stakeholderemail": "'"${stakeholderemail}"'",
      "stakeholdergroup": "'"${stakeholdergroup}"'",
      "secure_input_location": "'"${secure_input_location}"'",
      "secure_output_location": "'"${secure_output_location}"'"
    }
  }
}
'
#echo statements to double-check your values before running

echo "$JIRATicketURL"
echo "$ReasonForCipher"
echo "$StakeholderEmail"
echo "$StakeholderGroup"
echo "$TOTAL_INPUT_COLUMNS_COUNT"
echo "$OPER_INPUT_COLUMNS_COUNT"
echo "$OPERATION_TYPE"
echo "$TABLE_NAME"
echo "$COLUMN_NAME_LIST"
echo "$TODAY"
echo "$TODAY_FORMAT"
echo "$unq_id"
echo "$SECURE_INPUT_LOCATION"
echo "$SECURE_OUTPUT_LOCATION"

#this runs the lambda with the above payload
aws lambda invoke --function-name pi-qtm-dasp-${ENVIRONMENT}-cipher-util-run-job response --cli-binary-format raw-in-base64-out --payload "$payload_raw"

# The following adds similar tags to the cipher-util job by using it's EMR-ID (j-###########)

# use gac to find the emr-id and fill it out below
gac
emr_id=j-############

export tags=$(echo "RunUserName=\"${RunUserName}\" RunUserID=\"${RunUserID}\" jiraticketurl=\"${jiraticketurl}\" reasonforcipher=\"${reasonforcipher}\" stakeholderemail=\"${stakeholderemail}\" stakeholdergroup=\"${stakeholdergroup}\" table_name=\"${table_name}\" " | perl -pe 's/"+/"/g' );
at="aws emr add-tags --tags ${tags} --resource-id ${emr_id}"
echo $at
eval "$at"

#7. Return to your original terminal window and remove files

#Check Output Folder location for files
aws s3 ls s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/output/

#If you need the output TSV, download it to your machine before deleting from S3 Bucket:
aws s3 cp s3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/cipher_util/output/[JIRA Ticket Number]/[Filename] ~/documents/cipher_util_files/ # <-- your local folder

#Output folder cleanup:
aws s3 rm s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/cipher_util/output/[INSERT ADDED OUTPUT JIRA TICKET NUMBER AND FILENAME(S) HERE]



