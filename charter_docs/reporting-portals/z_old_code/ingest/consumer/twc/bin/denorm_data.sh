#!/bin/bash
DIR="$data_sources_path/twc_residential_global_adobe/${ENVIRONMENT}"

  rm -r -f $data_sources_path/twc_residential_global_adobe/${ENVIRONMENT}/denorm_process
  hive -e "truncate table $TMP_db.twc_residential_global_denorm_all_files"
# look for empty dir 
if [ "$(ls -A $DIR)" ]; then  
  ct=$(ls $data_sources_path/twc_residential_global_adobe/${ENVIRONMENT}/*-tsg2resglobal*.tsv |wc -l)
    echo "total file count is $ct"
    declare -i file_count
    file_count=1
    mkdir -p $data_sources_path/twc_residential_global_adobe/${ENVIRONMENT}/denorm_process
    for file in $(find $data_sources_path/twc_residential_global_adobe/${ENVIRONMENT}/ -type f -name '*tsg2resglobal*.tsv')
    do
      echo "Processing file_number $file_count for denorm--" $file
      hive -e "truncate table $TMP_db.twc_residential_global_raw"
      hive -e "truncate table $TMP_db.twc_residential_global_denorm"
      rm -rfv $data_sources_path/twc_residential_global/${ENVIRONMENT}/denorm_process/*  
      hadoop fs -rm -r /apps/hive/warehouse/$TMP_db.db/twc_residential_global_raw/
      hadoop fs -mkdir /apps/hive/warehouse/$TMP_db.db/twc_residential_global_raw/
      cp $file $data_sources_path/twc_residential_global_adobe/${ENVIRONMENT}/denorm_process/
      hadoop fs -copyFromLocal $file /apps/hive/warehouse/$TMP_db.db/twc_residential_global_raw/ 
      pig -f -useHCatalog src/twc_residential_global_denormalize.pig -param ENV=${ENVIRONMENT} -param FILE_COUNT=$file_count -param TMP_db=${TMP_db} -param LKP_db=${LKP_db} 
      hive -e "insert into table $TMP_db.twc_residential_global_denorm_all_files select * from $TMP_db.twc_residential_global_denorm"
      file_count=file_count+1
    done
else
   
  echo " $DIR is Empty"
       exit 1
       
fi