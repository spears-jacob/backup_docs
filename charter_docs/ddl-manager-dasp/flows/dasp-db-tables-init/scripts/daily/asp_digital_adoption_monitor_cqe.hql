CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_digital_adoption_monitor_cqe
(
  source_table string,
  date_type string,
  customer_type string,
  application_name string,
  visit_mso string,
  user_journey array<string>,
  user_sub_journey array<string>,
  grouping_id int,
  metric_name string,
  metric_count BIGINT,
  metric_count_distinct BiGINT,
  metric_total BIGINT,
  metric_count_pct double,
  run_time string
)
PARTITIONED BY (date_value STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
