CREATE EXTERNAL TABLE `si_lkp_encrypted_accounts`(
  `encrypted_acct_id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${s3_location}'
TBLPROPERTIES (
  'skip.header.line.count'='1')
