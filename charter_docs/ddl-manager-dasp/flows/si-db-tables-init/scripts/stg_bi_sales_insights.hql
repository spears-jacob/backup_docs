CREATE EXTERNAL TABLE ${db_name}.stg_bi_sales_insights(
  accountnumber string, 
  salesenterdate_i string, 
  ivr_activationattemptflag string, 
  ivr_activationsuccessflag string, 
  bundle_sale_type string, 
  billing_vendor string, 
  legacy_mso string, 
  ma string, 
  portal_activationattemptflag string, 
  portal_activationsuccessflag string, 
  region string, 
  resi_bundle_bi_category string, 
  saleschannel_exec_desc string, 
  sales_status string, 
  si_fulfillment_type string, 
  si_overall_activation_attempt_flag string, 
  si_overall_activation_success_flag string, 
  metrics string, 
  first_repair_call_occurred_before_overall_activation_attempt_flag string, 
  si_sales_orders_rescue_trtc_within_8day_order_entry string, 
  si_sales_orders_rescue_repair_within_8day_order_entry string, 
  salesenterdate date)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://pi-qtm-si-prod-aggregates-pii/data/tmp_prod_dasp/bi_sales_orders_insights'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1608604333')