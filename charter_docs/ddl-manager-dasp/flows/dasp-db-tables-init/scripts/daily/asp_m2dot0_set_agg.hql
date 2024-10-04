CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_m2dot0_set_agg (
  visit__application_details__application_name string, 
  visit__application_details__application_type string, 
  visit__application_details__app_version string, 
  agg_custom_visit__account__details__service_subscriptions string, 
  agg_custom_customer_group string, 
  agg_visit__account__configuration_factors string, 
  grouping_id int, 
  metric_name string, 
  metric_value decimal(12,4), 
  process_date_time_denver string, 
  process_identity string, 
  unit_type string, 
  call_count_24h int )
PARTITIONED BY (partition_date_utc STRING, grain STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
