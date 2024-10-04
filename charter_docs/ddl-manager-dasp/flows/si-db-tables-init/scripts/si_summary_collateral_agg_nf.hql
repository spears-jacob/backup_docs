CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.si_summary_collateral_agg_nf (
  billing_account_number string, 
  created_date string, 
  short_order_date string, 
  region string, 
  tpsi_ord_type string, 
  mso string, 
  work_order_date string, 
  application_group string, 
  user_package string, 
  si_flow_entry string, 
  order_full_success string, 
  order_partial_success string, 
  no_activation_success string, 
  activation_complete string, 
  connectivity_check string, 
  activate_call string, 
  activation_status string, 
  router_status string, 
  connectivity_check_modem string, 
  connectivity_check_router string, 
  connectivity_check_receiver string, 
  modem_success string, 
  router_success string, 
  receiver_success string, 
  voice_setup_success string, 
  activate_call_modem string, 
  activate_call_router string, 
  activate_call_receiver string, 
  digital_interaction string, 
  collateral string, 
  denver_date string, 
  modem_attempt string, 
  router_attempt string, 
  reciever_attempt string)
PARTITIONED BY ( 
  order_date string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://pi-qtm-si-stg-aggregates-pii/data/stg_dasp/si_summary_collateral_agg'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'orc.compress'='SNAPPY')