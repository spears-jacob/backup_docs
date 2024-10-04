CREATE EXTERNAL TABLE IF NOT EXISTS `${db_name}.quality_app_startup_base`(
  `visit_id` string
  ,`application_name` string 
  ,`gql_operation_name` string 
  ,`message_name` string
  ,`event_case_id` string
  ,`operation_success` boolean 
  ,`message_sequence_number` int 
  ,`message_timestamp` bigint
  ,`max_config_factors` string 
  ,`has_biometrics` boolean
  ,`current_page_name` string 
  ,`current_page_sequence_number` int 
  ,`operation_type` string
  ,`operation_int_value` int
  ,`app_version` string
  ,`application_type` string)
PARTITIONED BY ( 
  `denver_date` string)
STORED AS ORC 
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
