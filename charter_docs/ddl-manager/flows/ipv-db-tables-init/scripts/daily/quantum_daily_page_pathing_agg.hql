CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_page_pathing_agg
(
  current_page_name string,
  previous_page_name string,
  mso string,
  application_type string,
  select_action_types_value string,
  grouping_id int,
  metric_name string,
  metric_value double)
PARTITIONED BY (
    denver_date string)
STORED AS ORC
    LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
