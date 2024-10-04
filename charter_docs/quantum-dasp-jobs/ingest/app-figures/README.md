Runs on black cluster

-----------------
try the following to get files from PV incoming area back to PV by date of run, i.e. when nifi fails

#Connect to PV
ssh pve5

#make sure these are still good.  $working_folder is a local folder for your work
START_DATE=2022-01-29
END_DATE=2022-02-01
PV_src="/incoming/app_figures_raw/"
s3_destination="s3a://pi-qtm-dasp-prod-aggregates-nopii/data/prod_dasp/nifi/app_figures/archive/"
working_folder=~/work2

export owd=`pwd`
cd `mktemp -d`

#export production aws credentials
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_SESSION_TOKEN=""

#export copy alias
alias hdp_to_s3="hadoop distcp -Ddfs.client.use.datanode.hostname=true \
-Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider \
-Dfs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
-Dfs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
-Dfs.s3a.session.token=${AWS_SESSION_TOKEN} \
-Dfs.s3a.server-side-encryption-algorithm=SSE-KMS \
-Dfs.s3a.server-side-encryption.key='arn:aws:kms:us-east-1:387455165365:key/c226775d-0b8a-41a1-89bf-e5e7e361181f' \
-Dfs.s3a.acl.default=BucketOwnerFullControl \
-m 10 -overwrite"


export loopdate=$START_DATE
declare -a dates_array
while [[ $loopdate < "$END_DATE" ]] || [[ $loopdate = "$END_DATE" ]]
do
  dates_array+=($loopdate)
  export loopdate=$(date -d "$loopdate + 1 day" +%F)
done

hls -R -t $PV_src > all_incoming
egrep "*.gz" all_incoming > all_gz_files

for dutc in ${dates_array[@]}
do
  echo $dutc
  #pull out gz files by date modified
  egrep " $dutc " all_gz_files > $dutc.appfigures_files

  # clean up file prefixes
  while read line
  do
    from=$(echo "$line" | perl -pe 's|^.+ \/|hdfs://pi-datamart-ncw/|')
    to=$(echo "$line" | perl -pe "s|^.+ $PV_src|$s3_destination$dutc/|")
    echo "hdp_to_s3 $from $to" >> transfer_commands_$dutc
  done < $dutc.appfigures_files
done

ls trans*

# this folder, is a place where one can save and run the commands from
cp trans* $working_folder

cd $owd
unset owd

cd $working_folder

echo "Now run source transfer.*date*.sh to get the files to the correct folders"
