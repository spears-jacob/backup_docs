CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quality_m2_metric_agg(
  `application_name` string, 
  `application_type` string, 
  `application_version` string, 
  `mso` string, 
  `service_subscriptions` map<string,string>, 
  `account_number` string, 
  `billing_id` string, 
  `billing_division` string, 
  `visit_id` string, 
  `device_id` string, 
  `mobile_flag` string, 
  `auth_start` int, 
  `auth_success` int, 
  `auth_fail` int, 
  `high_home_page_load_time` int, 
  `acceptable_home_page_load_time` int, 
  `api_success` int, 
  `api_call` int, 
  `otp_mobile_start` int, 
  `otp_mobile_success` int, 
  `otp_mobile_fail` int, 
  `paymentmethod_start` int, 
  `paymentmethod_success` int, 
  `paymentmethod_fail` int, 
  `activation_start` int, 
  `activation_success` int, 
  `activation_fail` int, 
  `changeplan_start` int, 
  `changeplan_success` int, 
  `changeplan_attempt` int, 
  `otp_core_start` int, 
  `otp_core_success` int, 
  `otp_core_failure` int, 
  `autopay_start` int, 
  `autopay_success` int, 
  `autopay_failure` int, 
  `devicepayment_start` int, 
  `devicepayment_success` int, 
  `devicepayment_failure` int)
PARTITIONED BY ( 
  `denver_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
