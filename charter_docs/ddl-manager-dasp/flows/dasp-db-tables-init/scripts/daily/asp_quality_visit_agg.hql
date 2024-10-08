CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_quality_visit_agg (
  `visit_id` string,
  `account_number` string,
  `billing_id` string,
  `billing_division` string,
  `application_name` string,
  `application_version` string,
  `mso` string,
  `app_entry_score` double,
  `app_entry_login_failure_bucket` int,
  `app_entry_login_duration_bucket` int,
  `app_entry_page_load_bucket` int,
  `app_entry_login_failure_score` double,
  `app_entry_login_duration_score` double,
  `app_entry_page_load_score` double,
  `app_entry_success_count` int,
  `app_entry_fail_count` int,
  `app_entry_avg_duration_sec` double,
  `app_entry_avg_page_load_sec` double,
  `app_entry_has_success` boolean,
  `otp_score` double,
  `otp_transaction_duration_bucket` int,
  `otp_page_load_bucket` int,
  `otp_transaction_duration_score` double,
  `otp_page_load_score` double,
  `otp_success_count` int,
  `otp_fail_count` int,
  `otp_avg_transaction_min` double,
  `otp_avg_page_load_sec` double,
  `otp_has_success` boolean,
  `otp_more_than_allowed_fails` boolean,
  `ap_score` double,
  `ap_transaction_duration_bucket` int,
  `ap_page_load_bucket` int,
  `ap_transaction_duration_score` double,
  `ap_page_load_score` double,
  `ap_success_count` int,
  `ap_fail_count` int,
  `ap_enroll_success_count` int,
  `ap_manage_success_count` int,
  `ap_enroll_fail_count` int,
  `ap_manage_fail_count` int,
  `ap_avg_transaction_min` double,
  `ap_avg_page_load_sec` double,
  `ap_has_success` boolean,
  `ap_enroll_more_than_allowed_fails` boolean,
  `ap_manage_more_than_allowed_fails` boolean,
  `is_otp_abandon` boolean,
  `is_ap_abandon` boolean
)
PARTITIONED BY (denver_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
