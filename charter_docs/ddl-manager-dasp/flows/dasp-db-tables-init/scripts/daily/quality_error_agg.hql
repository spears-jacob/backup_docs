CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quality_error_agg (
  `application_name` string,
  `application_version` string,
  `application_type` string,
  `flow_group` string,
  `mso` string,
  `failure_bucket` string,
  `error_type` string,
  `error_text` string,
  `grouping_id` bigint,
  `custom_grouping_id` bigint,
  `metric_name` string,
  `metric_value` double,
  `script_version` string,
  `cluster_id` string
)
PARTITIONED BY (denver_date STRING, source_job STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
