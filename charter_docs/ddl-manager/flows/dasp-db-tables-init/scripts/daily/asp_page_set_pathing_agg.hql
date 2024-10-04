CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_page_set_pathing_agg (
  current_page_name string,
  previous_page_name string,
  mso string,
  application_name string,
  select_action_types_value string,
  grouping_id int,
  metric_name string,
  metric_value double)
PARTITIONED BY (denver_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;